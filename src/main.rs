use alloy::eips::BlockNumberOrTag;
use alloy::hex;
use alloy::primitives::U256;
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use clickhouse::query::Query;
use clickhouse::Client;
use log::{info, warn};
use reqwest::Url;
use serde_json::{Map, Value};
use std::io::{self, Write};

const DEFAULT_CLICKHOUSE_ADDRESS: &str = env!("CLICKHOUSE_ADDRESS");
const DEFAULT_CLICKHOUSE_PORT: &str = env!("CLICKHOUSE_PORT");
const DEFAULT_CLICKHOUSE_USER: &str = env!("CLICKHOUSE_USER");
const DEFAULT_CLICKHOUSE_PASSWORD: &str = env!("CLICKHOUSE_PASSWORD");
const DEFAULT_CLICKHOUSE_DATABASE: &str = env!("CLICKHOUSE_DATABASE");
const DEFAULT_ETH_NODE_URL: &str = env!("ETH_NODE_URL");
const TX_CHUNK_SIZE: usize = 10_000;
const DUPLICATE_SAMPLE_LIMIT: usize = 20;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Check Ethereum block and transaction coverage in ClickHouse"
)]
struct Args {
    /// ClickHouse base address, e.g. http://localhost
    #[arg(long, env = "CLICKHOUSE_ADDRESS", default_value = DEFAULT_CLICKHOUSE_ADDRESS)]
    address: String,
    /// ClickHouse HTTP port
    #[arg(long, env = "CLICKHOUSE_PORT", default_value = DEFAULT_CLICKHOUSE_PORT)]
    port: u16,
    /// ClickHouse user name
    #[arg(long, env = "CLICKHOUSE_USER", default_value = DEFAULT_CLICKHOUSE_USER)]
    user: String,
    /// ClickHouse password
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = DEFAULT_CLICKHOUSE_PASSWORD)]
    password: String,
    /// ClickHouse database name (schema)
    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = DEFAULT_CLICKHOUSE_DATABASE)]
    database: String,
    /// Blocks table name
    #[arg(long, env = "BLOCKS_TABLE", default_value = "blocks")]
    blocks_table: String,
    /// Column name that stores the block number in the blocks table
    #[arg(long, env = "BLOCKS_NUMBER_COLUMN", default_value = "number")]
    blocks_number_column: String,
    /// Transactions table name
    #[arg(long, env = "TRANSACTIONS_TABLE", default_value = "transactions")]
    transactions_table: String,
    /// Column name that stores the block number in the transactions table
    #[arg(
        long,
        env = "TRANSACTIONS_BLOCK_COLUMN",
        default_value = "block_number"
    )]
    transactions_block_column: String,
    /// Ethereum node RPC endpoint
    #[arg(long, env = "ETH_NODE_URL", default_value = DEFAULT_ETH_NODE_URL)]
    eth_node_url: String,
}

#[derive(Debug)]
struct BlockStats {
    min: u64,
    max: u64,
    distinct_count: u64,
    total_rows: u64,
}

#[derive(Debug)]
struct TxMismatch {
    block_number: u64,
    node_tx_count: u64,
    clickhouse_tx_count: u64,
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    column_type: String,
}

#[derive(Debug)]
struct BlockDuplicateReport {
    total_extra_rows: u64,
    samples: Vec<BlockDuplicateEntry>,
    truncated: bool,
}

#[derive(Debug)]
struct BlockDuplicateEntry {
    block_number: u64,
    occurrences: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationState {
    Pending,
    Failed,
    Finished,
    Killed,
}

#[derive(Debug)]
struct MutationInfo {
    table: String,
    mutation_id: String,
    command: String,
    create_time: String,
    state: MutationState,
    parts_remaining: Option<u64>,
    parts_total: Option<u64>,
    latest_failed_reason: Option<String>,
}

impl MutationState {
    fn label(self) -> &'static str {
        match self {
            MutationState::Pending => "pending",
            MutationState::Failed => "failed",
            MutationState::Finished => "finished",
            MutationState::Killed => "killed",
        }
    }

    fn is_actionable(self) -> bool {
        matches!(self, MutationState::Pending | MutationState::Failed)
    }
}

#[derive(Debug, Clone)]
struct TableMetadata {
    engine: Option<String>,
    apply_final: bool,
}

#[derive(Debug, Clone, Copy)]
struct SelectedChecks {
    block_gap: bool,
    tx_gap: bool,
    tx_mismatch: bool,
    duplicates: bool,
    mutations: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    init_logging();

    let checks = prompt_check_selection()?;

    validate_identifiers(&args)?;

    let base_url = build_clickhouse_url(&args.address, args.port);

    let client = Client::default()
        .with_url(&base_url)
        .with_user(&args.user)
        .with_password(&args.password)
        .with_database(&args.database);

    let block_columns = print_table_schema(&client, &args.blocks_table, "blocks").await?;
    let tx_columns = print_table_schema(&client, &args.transactions_table, "transactions").await?;

    let blocks_meta = fetch_table_metadata(&client, &args.blocks_table).await?;
    if blocks_meta.apply_final {
        if let Some(engine) = &blocks_meta.engine {
            println!(
                "Table `{}` uses engine `{}`; applying FINAL for consistency.",
                args.blocks_table, engine
            );
        } else {
            println!(
                "Table `{}` requires FINAL for consistent reads; applying FINAL.",
                args.blocks_table
            );
        }
    }

    let tx_meta = fetch_table_metadata(&client, &args.transactions_table).await?;
    if tx_meta.apply_final {
        if let Some(engine) = &tx_meta.engine {
            println!(
                "Table `{}` uses engine `{}`; applying FINAL for consistency.",
                args.transactions_table, engine
            );
        } else {
            println!(
                "Table `{}` requires FINAL for consistent reads; applying FINAL.",
                args.transactions_table
            );
        }
    }

    ensure_column_exists(
        &block_columns,
        &args.blocks_number_column,
        &args.blocks_table,
        "--blocks-number-column",
    )?;
    ensure_column_exists(
        &tx_columns,
        &args.transactions_block_column,
        &args.transactions_table,
        "--transactions-block-column",
    )?;

    if checks.mutations {
        report_unfinished_mutations(&client).await?;
    }

    let mut provider: Option<RootProvider> = None;

    let block_stats = match fetch_table_stats(
        &client,
        &args.blocks_table,
        &args.blocks_number_column,
        blocks_meta.apply_final,
    )
    .await?
    {
        None => {
            println!(
                "No blocks found in table `{}`. Nothing to check.",
                args.blocks_table
            );
            return Ok(());
        }
        Some(stats) => stats,
    };

    println!(
        "Block range: {}…{} ({} distinct rows, {} total rows)",
        block_stats.min, block_stats.max, block_stats.distinct_count, block_stats.total_rows
    );

    if checks.duplicates && block_stats.total_rows > block_stats.distinct_count {
        println!(
            "Warning: found {} duplicate block rows.",
            block_stats.total_rows - block_stats.distinct_count
        );
        if let Some(duplicates) =
            find_duplicate_blocks(&client, &args, blocks_meta.apply_final).await?
        {
            println!(
                "Detected {} duplicate row(s) across {} block number(s):",
                duplicates.total_extra_rows,
                duplicates.samples.len()
            );
            for entry in &duplicates.samples {
                println!(
                    "  block {} appears {} times",
                    entry.block_number, entry.occurrences
                );
            }
            if duplicates.truncated {
                println!(
                    "  ...and more (showing first {} block numbers).",
                    DUPLICATE_SAMPLE_LIMIT
                );
            }
        }
    }

    if checks.block_gap {
        let span = block_stats
            .max
            .checked_sub(block_stats.min)
            .context("Block number range is invalid")?;
        let expected_blocks = span.checked_add(1).context("Block number range overflow")?;

        if expected_blocks == block_stats.distinct_count {
            println!(
                "No gaps detected between {} and {} in blocks table `{}`.",
                block_stats.min, block_stats.max, args.blocks_table
            );
        } else {
            let missing_ranges = find_missing_ranges(
                &client,
                &args.blocks_table,
                &args.blocks_number_column,
                blocks_meta.apply_final,
            )
            .await?;
            let context = format!("blocks table `{}`", args.blocks_table);
            report_missing_ranges(&missing_ranges, &context);

            if !missing_ranges.is_empty() {
                if prompt_fill_missing()? {
                    println!("欠損ブロックをEthereumノードから取得し、ClickHouseに補完します。");
                    let provider_ref = ensure_provider(&mut provider, &args.eth_node_url)?;
                    fill_missing_blocks_and_transactions(
                        &client,
                        provider_ref,
                        &args,
                        &block_columns,
                        &tx_columns,
                        &missing_ranges,
                    )
                    .await?;
                } else {
                    println!("ユーザー入力により欠損データの補完をスキップしました。");
                }
            }
        }
    }

    if checks.tx_gap {
        info!(
            "Starting transaction table gap detection for `{}` (block column = `{}`).",
            args.transactions_table, args.transactions_block_column
        );
        match fetch_table_stats(
            &client,
            &args.transactions_table,
            &args.transactions_block_column,
            tx_meta.apply_final,
        )
        .await?
        {
            None => {
                info!(
                    "Transaction table `{}` returned no rows; skipping gap detection.",
                    args.transactions_table
                );
                println!(
                    "No transactions found in table `{}`. Skipping transaction gap check.",
                    args.transactions_table
                );
            }
            Some(tx_stats) => {
                info!(
                    "Loaded transaction stats: range {}…{}, {} distinct block number(s), {} total row(s).",
                    tx_stats.min, tx_stats.max, tx_stats.distinct_count, tx_stats.total_rows
                );
                println!(
                    "Transaction block range: {}…{} ({} distinct block numbers, {} total rows)",
                    tx_stats.min, tx_stats.max, tx_stats.distinct_count, tx_stats.total_rows
                );
                let span = tx_stats
                    .max
                    .checked_sub(tx_stats.min)
                    .context("Transaction block number range is invalid")?;
                let expected_blocks = span
                    .checked_add(1)
                    .context("Transaction block number range overflow")?;
                if expected_blocks == tx_stats.distinct_count {
                    info!(
                        "No transaction block gaps detected between {} and {} in `{}`.",
                        tx_stats.min, tx_stats.max, args.transactions_table
                    );
                    println!(
                        "No gaps detected between {} and {} in transactions table `{}`.",
                        tx_stats.min, tx_stats.max, args.transactions_table
                    );
                } else {
                    info!(
                        "Scanning `{}` for missing transaction block ranges...",
                        args.transactions_table
                    );
                    let missing_ranges = find_missing_ranges(
                        &client,
                        &args.transactions_table,
                        &args.transactions_block_column,
                        tx_meta.apply_final,
                    )
                    .await?;
                    let missing_total: u64 = missing_ranges
                        .iter()
                        .map(|(start, end)| if end < start { 0 } else { end - start + 1 })
                        .sum();
                    info!(
                        "Detected {} missing transaction block(s) across {} gap(s).",
                        missing_total,
                        missing_ranges.len()
                    );
                    let context = format!("transactions table `{}`", args.transactions_table);
                    report_missing_ranges(&missing_ranges, &context);
                    info!(
                        "Finished transaction gap detection for `{}`.",
                        args.transactions_table
                    );
                }
            }
        }
    }

    if checks.tx_mismatch {
        let provider_ref = ensure_provider(&mut provider, &args.eth_node_url)?;
        let mismatches = find_transaction_mismatches(
            &client,
            &args,
            provider_ref,
            blocks_meta.apply_final,
            tx_meta.apply_final,
        )
        .await?;
        if mismatches.is_empty() {
            println!(
                "All block transaction counts match between ClickHouse and the Ethereum node."
            );
        } else {
            println!(
                "Found {} block(s) with mismatched transaction counts:",
                mismatches.len()
            );
            for mismatch in &mismatches {
                println!(
                    "  block {}: node={} clickhouse={}",
                    mismatch.block_number, mismatch.node_tx_count, mismatch.clickhouse_tx_count
                );
            }
            if prompt_repair_mismatches()? {
                println!("検出された不整合ブロックをEthereumノードのデータで補修します。");
                repair_transaction_mismatches(
                    &client,
                    provider_ref,
                    &args,
                    &block_columns,
                    &tx_columns,
                    &mismatches,
                )
                .await?;
            } else {
                println!("ユーザー入力によりトランザクション補修をスキップしました。");
            }
        }
    }

    if checks.mutations {
        cleanup_mutations(&client, &args).await?;
    }

    Ok(())
}

fn validate_identifiers(args: &Args) -> Result<()> {
    ensure_identifier(&args.blocks_table, "blocks table name")?;
    ensure_identifier(&args.blocks_number_column, "blocks.number column")?;
    ensure_identifier(&args.transactions_table, "transactions table name")?;
    ensure_identifier(
        &args.transactions_block_column,
        "transactions.block_number column",
    )?;
    Ok(())
}

fn ensure_identifier(value: &str, context: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value
            .chars()
            .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_'));
    if valid {
        Ok(())
    } else {
        bail!("{context} must contain only ASCII letters, digits, or underscores: `{value}`");
    }
}

async fn fetch_table_stats(
    client: &Client,
    table: &str,
    column: &str,
    use_final: bool,
) -> Result<Option<BlockStats>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct StatsRow {
        min_number: Option<u64>,
        max_number: Option<u64>,
        distinct_blocks: u64,
        total_rows: u64,
    }

    let final_clause = final_clause(use_final);
    let query = format!(
        "SELECT \
            minOrNull({col}) AS min_number, \
            maxOrNull({col}) AS max_number, \
            uniqExact({col}) AS distinct_blocks, \
            count() AS total_rows \
         FROM {table}{final_clause}",
        col = column,
        table = table,
        final_clause = final_clause
    );

    let rows: Vec<StatsRow> = client.query(&query).fetch_all().await?;

    let stats_row = rows.into_iter().next().unwrap_or(StatsRow {
        min_number: None,
        max_number: None,
        distinct_blocks: 0,
        total_rows: 0,
    });

    match (
        stats_row.min_number,
        stats_row.max_number,
        stats_row.distinct_blocks,
        stats_row.total_rows,
    ) {
        (Some(min), Some(max), distinct_count, total_rows) if distinct_count > 0 => {
            Ok(Some(BlockStats {
                min,
                max,
                distinct_count,
                total_rows,
            }))
        }
        _ => Ok(None),
    }
}

async fn find_missing_ranges(
    client: &Client,
    table: &str,
    column: &str,
    use_final: bool,
) -> Result<Vec<(u64, u64)>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct GapRow {
        gap_start: u64,
        gap_end: u64,
    }

    let final_clause = final_clause(use_final);
    let query = format!(
        "SELECT \
            toUInt64(assumeNotNull(prev_block) + 1) AS gap_start, \
            toUInt64(current_block - 1) AS gap_end \
         FROM ( \
            SELECT \
                {col} AS current_block, \
                lag({col}) OVER (ORDER BY {col}) AS prev_block \
            FROM {table}{final_clause} \
         ) \
         WHERE prev_block IS NOT NULL \
           AND current_block - assumeNotNull(prev_block) > 1 \
         ORDER BY gap_start",
        col = column,
        table = table,
        final_clause = final_clause
    );

    info!(
        "Executing gap detection query for table `{}` (ordering by `{}`).",
        table, column
    );
    let rows: Vec<GapRow> = client.query(&query).fetch_all().await?;
    info!(
        "Received {} gap candidate row(s) from table `{}`.",
        rows.len(),
        table
    );
    let mut ranges = Vec::with_capacity(rows.len());

    for (idx, row) in rows.into_iter().enumerate() {
        if row.gap_end < row.gap_start {
            bail!(
                "ClickHouse returned inverted gap range {}-{} for table `{}`",
                row.gap_start,
                row.gap_end,
                table
            );
        }
        ranges.push((row.gap_start, row.gap_end));
        if (idx + 1) % 1_000 == 0 {
            info!(
                "Processed {} gap candidate row(s) from table `{}` so far.",
                idx + 1,
                table
            );
        }
    }

    Ok(ranges)
}

fn report_missing_ranges(ranges: &[(u64, u64)], context: &str) {
    if ranges.is_empty() {
        println!("No block gaps found in {context}.");
        return;
    }

    let missing_total: u64 = ranges
        .iter()
        .map(|(start, end)| if end < start { 0 } else { end - start + 1 })
        .sum();

    println!(
        "Detected {} missing block(s) across {} gap(s) in {}:",
        missing_total,
        ranges.len(),
        context
    );
    for (start, end) in ranges {
        if start == end {
            println!("  missing block {}", start);
        } else {
            println!("  missing blocks {}-{}", start, end);
        }
    }
}

async fn find_duplicate_blocks(
    client: &Client,
    args: &Args,
    use_final: bool,
) -> Result<Option<BlockDuplicateReport>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct DuplicateCountRow {
        duplicate_rows: i64,
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct DuplicateSampleRow {
        block_number: u64,
        occurrences: u64,
    }

    let final_clause = final_clause(use_final);
    let count_query = format!(
        "SELECT ifNull(sum(occurrences - 1), 0) AS duplicate_rows \
         FROM ( \
            SELECT count() AS occurrences \
            FROM {table}{final_clause} \
            GROUP BY {col} \
            HAVING occurrences > 1 \
         )",
        table = args.blocks_table,
        col = args.blocks_number_column,
        final_clause = final_clause
    );

    let duplicate_rows_raw: i64 = client
        .query(&count_query)
        .fetch_one::<DuplicateCountRow>()
        .await?
        .duplicate_rows;

    let duplicate_rows = u64::try_from(duplicate_rows_raw).map_err(|_| {
        anyhow!(
            "ClickHouse returned negative duplicate row count ({}) for table `{}`",
            duplicate_rows_raw,
            args.blocks_table
        )
    })?;

    if duplicate_rows == 0 {
        return Ok(None);
    }

    let sample_query = format!(
        "SELECT \
            {col} AS block_number, \
            count() AS occurrences \
         FROM {table}{final_clause} \
         GROUP BY {col} \
         HAVING occurrences > 1 \
         ORDER BY block_number \
         LIMIT {limit}",
        col = args.blocks_number_column,
        table = args.blocks_table,
        limit = DUPLICATE_SAMPLE_LIMIT + 1,
        final_clause = final_clause
    );

    let mut rows: Vec<DuplicateSampleRow> = client.query(&sample_query).fetch_all().await?;
    let truncated = rows.len() > DUPLICATE_SAMPLE_LIMIT;
    if truncated {
        rows.truncate(DUPLICATE_SAMPLE_LIMIT);
    }

    let samples = rows
        .into_iter()
        .map(|row| BlockDuplicateEntry {
            block_number: row.block_number,
            occurrences: row.occurrences,
        })
        .collect();

    Ok(Some(BlockDuplicateReport {
        total_extra_rows: duplicate_rows,
        samples,
        truncated,
    }))
}

async fn find_transaction_mismatches<P>(
    client: &Client,
    args: &Args,
    provider: &P,
    blocks_use_final: bool,
    tx_use_final: bool,
) -> Result<Vec<TxMismatch>>
where
    P: Provider,
{
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct BlockTxRow {
        block_number: u64,
        clickhouse_tx_count: u64,
    }

    let mut mismatches = Vec::new();
    let mut processed_blocks: u64 = 0;
    let mut last_block: Option<u64> = None;

    info!(
        "Starting transaction count verification for blocks in `{}` (chunk size = {}).",
        args.blocks_table, TX_CHUNK_SIZE
    );

    loop {
        let block_filter = match last_block {
            Some(block) => format!("WHERE {col} > {block}", col = args.blocks_number_column),
            None => String::new(),
        };
        let tx_filter = match last_block {
            Some(block) => format!(
                "WHERE {trans_block} > {block}",
                trans_block = args.transactions_block_column
            ),
            None => String::new(),
        };

        let blocks_final_clause = final_clause(blocks_use_final);
        let tx_final_clause = final_clause(tx_use_final);

        let query = format!(
            "SELECT \
                b.block_number AS block_number, \
                ifNull(t.tx_count, 0) AS clickhouse_tx_count \
             FROM ( \
                SELECT {block_col} AS block_number \
                FROM {blocks_table}{blocks_final_clause} \
                {block_filter} \
                GROUP BY block_number \
                ORDER BY block_number \
                LIMIT {limit} \
             ) AS b \
             LEFT JOIN ( \
                SELECT \
                    {trans_block} AS block_number, \
                    count() AS tx_count \
                FROM {transactions_table}{tx_final_clause} \
                {tx_filter} \
                GROUP BY {trans_block} \
             ) AS t USING block_number \
             ORDER BY block_number",
            block_col = args.blocks_number_column,
            blocks_table = args.blocks_table,
            blocks_final_clause = blocks_final_clause,
            block_filter = block_filter,
            limit = TX_CHUNK_SIZE,
            trans_block = args.transactions_block_column,
            transactions_table = args.transactions_table,
            tx_final_clause = tx_final_clause,
            tx_filter = tx_filter
        );

        let rows: Vec<BlockTxRow> =
            client
                .query(&query)
                .fetch_all()
                .await
                .with_context(|| match last_block {
                    Some(block) => format!(
                        "Failed to load ClickHouse transaction counts after block {}",
                        block
                    ),
                    None => "Failed to load initial ClickHouse transaction counts".to_string(),
                })?;

        if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
            println!(
                "Scanning transaction counts for blocks {}..{} ({} block(s))",
                first.block_number,
                last.block_number,
                rows.len()
            );
        }

        if rows.is_empty() {
            break;
        }

        for row in rows {
            last_block = Some(row.block_number);
            processed_blocks += 1;

            let node_count = provider
                .get_block_transaction_count_by_number(BlockNumberOrTag::Number(row.block_number))
                .await
                .with_context(|| {
                    format!(
                        "Unable to query node for block {} transaction count",
                        row.block_number
                    )
                })?
                .ok_or_else(|| {
                    anyhow!(
                        "Ethereum node returned no data for block {}",
                        row.block_number
                    )
                })?;

            if node_count != row.clickhouse_tx_count {
                mismatches.push(TxMismatch {
                    block_number: row.block_number,
                    node_tx_count: node_count,
                    clickhouse_tx_count: row.clickhouse_tx_count,
                });
            }

            if processed_blocks.is_multiple_of(100) {
                info!(
                    "Checked transaction counts for {} blocks...",
                    processed_blocks
                );
                println!(
                    "Checked transaction counts for {} block(s)...",
                    processed_blocks
                );
            }
        }
    }

    match processed_blocks {
        0 => info!("No blocks found to verify transaction counts."),
        n => info!("Finished checking transaction counts for {} block(s).", n),
    }

    Ok(mismatches)
}

async fn cleanup_mutations(client: &Client, args: &Args) -> Result<()> {
    println!(
        "Inspecting ClickHouse mutations for `{}` and `{}`...",
        args.blocks_table, args.transactions_table
    );

    let mut tables = Vec::new();
    tables.push(args.blocks_table.clone());
    if args.transactions_table != args.blocks_table {
        tables.push(args.transactions_table.clone());
    }

    let mutations = fetch_mutation_info(client, &tables).await?;

    if mutations.is_empty() {
        println!("No unfinished or failed mutations found for the target tables.");
        return Ok(());
    }

    println!(
        "Detected {} mutation(s) for the target tables:",
        mutations.len()
    );
    for (idx, info) in mutations.iter().enumerate() {
        println!(
            "  {}. [{}] mutation {} on `{}` (created {})",
            idx + 1,
            info.state.label(),
            info.mutation_id,
            info.table,
            info.create_time
        );

        if let Some(parts_total) = info.parts_total {
            match info.parts_remaining {
                Some(remaining) => {
                    println!(
                        "       parts remaining: {} / total: {}",
                        remaining, parts_total
                    );
                }
                None => {
                    println!("       parts total: {}", parts_total);
                }
            }
        }

        if let Some(reason) = &info.latest_failed_reason {
            if !reason.trim().is_empty() {
                println!("       last failure: {}", reason);
            }
        }

        println!("       command: {}", summarize_command(&info.command));
    }

    let actionable: Vec<&MutationInfo> = mutations
        .iter()
        .filter(|info| info.state.is_actionable())
        .collect();

    if !actionable.is_empty() {
        println!(
            "{} mutation(s) remain pending or failed. Consider shrinking their scope with smaller DELETE batches or wait for ClickHouse to finish them.",
            actionable.len()
        );
    }

    let mut target_tables: Vec<(String, String)> = Vec::new();
    target_tables.push((args.blocks_table.clone(), args.blocks_number_column.clone()));
    if !target_tables.iter().any(|(table, column)| {
        table == &args.transactions_table && column == &args.transactions_block_column
    }) {
        target_tables.push((
            args.transactions_table.clone(),
            args.transactions_block_column.clone(),
        ));
    }

    for (table, column) in &target_tables {
        let prompt = format!(
            "Queue targeted `ALTER TABLE {table} DELETE WHERE {column} ...` operations now?"
        );
        if prompt_yes_no(&prompt)? {
            run_targeted_deletes(client, table, column).await?;
        }
    }

    for table in tables {
        if prompt_yes_no(&format!("Run `OPTIMIZE TABLE {} FINAL` now?", table))? {
            optimize_table(client, &table).await?;
            println!("  OPTIMIZE TABLE {} FINAL issued.", table);
        }
    }

    Ok(())
}

async fn fetch_mutation_info(client: &Client, tables: &[String]) -> Result<Vec<MutationInfo>> {
    if tables.is_empty() {
        return Ok(Vec::new());
    }

    let table_filter = tables
        .iter()
        .map(|table| format!("'{}'", table.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct RawMutationRow {
        table: String,
        mutation_id: String,
        command: String,
        create_time: String,
        is_done: u8,
        is_killed: u8,
        latest_failed_part: Option<String>,
        latest_failed_part_why: Option<String>,
        parts_to_do: Option<u64>,
        parts_done: Option<u64>,
    }

    let query = format!(
        "SELECT \
            table, \
            mutation_id, \
            command, \
            toString(create_time) AS create_time, \
            is_done, \
            is_killed, \
            latest_failed_part, \
            latest_failed_part_why, \
            parts_to_do, \
            parts_done \
         FROM system.mutations \
         WHERE database = currentDatabase() \
           AND table IN ({}) \
           AND (is_done = 0 OR is_killed = 1 OR latest_failed_part != '') \
         ORDER BY create_time",
        table_filter
    );

    let rows: Vec<RawMutationRow> = client.query(&query).fetch_all().await?;
    let mut result = Vec::with_capacity(rows.len());

    for row in rows {
        let state = if row.is_killed != 0 {
            MutationState::Killed
        } else if row.is_done != 0 {
            MutationState::Finished
        } else if row
            .latest_failed_part
            .as_ref()
            .map(|part| !part.is_empty())
            .unwrap_or(false)
        {
            MutationState::Failed
        } else {
            MutationState::Pending
        };

        let parts_remaining = match (row.parts_to_do, row.parts_done) {
            (Some(todo), Some(done)) => Some(todo.saturating_sub(done)),
            (Some(todo), None) => Some(todo),
            _ => None,
        };

        let latest_failed_reason = row.latest_failed_part_why.and_then(|why| {
            if why.trim().is_empty() {
                None
            } else {
                Some(why)
            }
        });

        result.push(MutationInfo {
            table: row.table,
            mutation_id: row.mutation_id,
            command: row.command,
            create_time: row.create_time,
            state,
            parts_remaining,
            parts_total: row.parts_to_do,
            latest_failed_reason,
        });
    }

    Ok(result)
}

async fn optimize_table(client: &Client, table: &str) -> Result<()> {
    let query = format!("OPTIMIZE TABLE {} FINAL", table);

    client
        .query(&query)
        .execute()
        .await
        .with_context(|| format!("Failed to optimize table `{}`", table))?;

    Ok(())
}

async fn run_targeted_deletes(client: &Client, table: &str, column: &str) -> Result<()> {
    println!(
        "Provide block numbers or ranges to delete from `{}` (column `{}`).",
        table, column
    );
    println!("  Format: use `12345` or `12340-12350`, separated by commas.");

    let mut executed = false;

    loop {
        let prompt = format!(
            "Enter block numbers/ranges for `{}` (empty to finish): ",
            table
        );
        let ranges = prompt_block_ranges(&prompt)?;

        if ranges.is_empty() {
            if executed {
                println!(
                    "  No additional ranges provided. Finishing for `{}`.",
                    table
                );
            } else {
                println!(
                    "  No ranges provided. Skipping targeted DELETE for `{}`.",
                    table
                );
            }
            break;
        }

        for (start, end) in ranges {
            submit_delete_range(client, table, column, start, end).await?;
            executed = true;
        }

        if !prompt_yes_no("Queue more ranges for this table?")? {
            break;
        }
    }

    Ok(())
}

fn prompt_block_ranges(prompt: &str) -> Result<Vec<(u64, u64)>> {
    print!("{}", prompt);
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    let read = io::stdin()
        .read_line(&mut input)
        .context("Failed to read block range input")?;

    if read == 0 || input.trim().is_empty() {
        return Ok(Vec::new());
    }

    parse_block_ranges(&input)
}

fn parse_block_ranges(input: &str) -> Result<Vec<(u64, u64)>> {
    let mut ranges = Vec::new();

    for token in input.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (start, end) = if let Some((start_str, end_str)) = trimmed.split_once('-') {
            let start: u64 = start_str
                .trim()
                .parse()
                .with_context(|| format!("Invalid block number `{}`", start_str.trim()))?;
            let end: u64 = end_str
                .trim()
                .parse()
                .with_context(|| format!("Invalid block number `{}`", end_str.trim()))?;
            if start > end {
                bail!("Block range {}-{} is inverted", start, end);
            }
            (start, end)
        } else {
            let value: u64 = trimmed
                .parse()
                .with_context(|| format!("Invalid block number `{}`", trimmed))?;
            (value, value)
        };

        ranges.push((start, end));
    }

    ranges.sort_by_key(|(start, _)| *start);

    Ok(ranges)
}

async fn submit_delete_range(
    client: &Client,
    table: &str,
    column: &str,
    start: u64,
    end: u64,
) -> Result<()> {
    let condition = if start == end {
        format!("{} = {}", column, start)
    } else {
        format!("{} BETWEEN {} AND {}", column, start, end)
    };

    println!(
        "  Executing ALTER TABLE {} DELETE WHERE {} (synchronous)...",
        table, condition
    );

    let query = format!(
        "ALTER TABLE {} DELETE WHERE {} SETTINGS mutations_sync = 1",
        table, condition
    );

    client.query(&query).execute().await.with_context(|| {
        format!(
            "Failed to execute DELETE on `{}` for block range {}-{}",
            table, start, end
        )
    })?;

    println!(
        "    Completed deletion for range {}-{} on `{}`.",
        start, end, table
    );

    Ok(())
}

fn summarize_command(command: &str) -> String {
    let condensed = command
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");

    if condensed.is_empty() {
        return "(empty command)".to_string();
    }

    const MAX_LEN: usize = 160;
    if condensed.chars().count() > MAX_LEN {
        let truncated = condensed.chars().take(MAX_LEN - 3).collect::<String>();
        format!("{}...", truncated)
    } else {
        condensed
    }
}

fn prompt_yes_no(question: &str) -> Result<bool> {
    loop {
        print!("{} [y/N]: ", question);
        io::stdout().flush().context("Failed to flush stdout")?;

        let mut input = String::new();
        let read = io::stdin()
            .read_line(&mut input)
            .context("Failed to read response from stdin")?;

        if read == 0 {
            return Ok(false);
        }

        let trimmed = input.trim().to_lowercase();
        if trimmed.is_empty() || trimmed == "n" || trimmed == "no" {
            return Ok(false);
        }
        if trimmed == "y" || trimmed == "yes" {
            return Ok(true);
        }

        println!("Please answer with `y` or `n`.");
    }
}

async fn fetch_table_columns(client: &Client, table: &str) -> Result<Vec<ColumnInfo>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct RawColumn {
        name: String,
        column_type: String,
    }

    let query = format!(
        "SELECT name, type AS column_type \
         FROM system.columns \
         WHERE database = currentDatabase() \
           AND table = '{table}' \
         ORDER BY position",
        table = table
    );

    let rows: Vec<RawColumn> = client.query(&query).fetch_all().await?;
    Ok(rows
        .into_iter()
        .map(|row| ColumnInfo {
            name: row.name,
            column_type: row.column_type,
        })
        .collect())
}

#[derive(Debug, Clone)]
struct ColumnValue {
    name: String,
    value: QueryValue,
}

#[derive(Debug, Clone)]
enum QueryValue {
    UInt64(u64),
    NullableUInt64(Option<u64>),
    Int64(i64),
    NullableInt64(Option<i64>),
    Float64(f64),
    NullableFloat64(Option<f64>),
    String(String),
    NullableString(Option<String>),
    Bool(bool),
    NullableBool(Option<bool>),
    Bytes(Vec<u8>),
    NullableBytes(Option<Vec<u8>>),
    ArrayString(Vec<String>),
    NullableArrayString(Option<Vec<String>>),
}

impl QueryValue {
    fn bind(self, query: Query) -> Query {
        match self {
            QueryValue::UInt64(v) => query.bind(v),
            QueryValue::NullableUInt64(v) => query.bind(v),
            QueryValue::Int64(v) => query.bind(v),
            QueryValue::NullableInt64(v) => query.bind(v),
            QueryValue::Float64(v) => query.bind(v),
            QueryValue::NullableFloat64(v) => query.bind(v),
            QueryValue::String(v) => query.bind(v),
            QueryValue::NullableString(v) => query.bind(v),
            QueryValue::Bool(v) => query.bind(v),
            QueryValue::NullableBool(v) => query.bind(v),
            QueryValue::Bytes(v) => query.bind(v),
            QueryValue::NullableBytes(v) => query.bind(v),
            QueryValue::ArrayString(v) => query.bind(v),
            QueryValue::NullableArrayString(v) => query.bind(v),
        }
    }
}

#[derive(Debug, Clone)]
struct ColumnTypeInfo {
    base: BaseType,
    nullable: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum BaseType {
    UInt(u16),
    Int(u16),
    Float(u8),
    String,
    FixedString(usize),
    Bool,
    DateTime,
    DateTime64(u32),
    ArrayString,
    Decimal { precision: u32, scale: u32 },
    Unknown(String),
}

fn ensure_provider<'a>(
    provider: &'a mut Option<RootProvider>,
    eth_node_url: &str,
) -> Result<&'a RootProvider> {
    if provider.is_none() {
        let eth_url = Url::parse(eth_node_url)
            .with_context(|| format!("Invalid ETH node URL: {}", eth_node_url))?;
        let new_provider = ProviderBuilder::default().connect_http(eth_url);
        *provider = Some(new_provider);
    }

    Ok(provider
        .as_ref()
        .expect("provider must exist after initialization"))
}

fn prompt_fill_missing() -> Result<bool> {
    println!("検出された欠損ブロックを補完しますか？ (y/N)");
    print!("> ");
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("Failed to read confirmation for filling missing blocks")?;

    let normalized = input.trim().to_lowercase();
    let decision = matches!(normalized.as_str(), "y" | "yes" | "1" | "true" | "はい");
    Ok(decision)
}

fn prompt_repair_mismatches() -> Result<bool> {
    println!("検出されたトランザクション不整合を補修しますか？ (y/N)");
    print!("> ");
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("Failed to read confirmation for repairing mismatched transactions")?;

    let normalized = input.trim().to_lowercase();
    let decision = matches!(normalized.as_str(), "y" | "yes" | "1" | "true" | "はい");
    Ok(decision)
}

fn collapse_block_numbers_to_ranges(block_numbers: &[u64]) -> Vec<(u64, u64)> {
    if block_numbers.is_empty() {
        return Vec::new();
    }

    let mut sorted = block_numbers.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut ranges = Vec::new();
    let mut iter = sorted.into_iter();
    if let Some(mut start) = iter.next() {
        let mut end = start;
        for num in iter {
            if end.checked_add(1) == Some(num) {
                end = num;
            } else {
                ranges.push((start, end));
                start = num;
                end = num;
            }
        }
        ranges.push((start, end));
    }

    ranges
}

async fn repair_transaction_mismatches(
    client: &Client,
    provider: &impl Provider,
    args: &Args,
    block_columns: &[ColumnInfo],
    tx_columns: &[ColumnInfo],
    mismatches: &[TxMismatch],
) -> Result<()> {
    let block_numbers: Vec<u64> = mismatches.iter().map(|m| m.block_number).collect();
    let ranges = collapse_block_numbers_to_ranges(&block_numbers);
    if ranges.is_empty() {
        return Ok(());
    }

    let total_blocks = ranges.iter().try_fold(0u64, |acc, &(start, end)| {
        let span = end
            .checked_sub(start)
            .and_then(|diff| diff.checked_add(1))
            .ok_or_else(|| anyhow!("ブロック範囲 {}-{} のサイズ計算に失敗しました", start, end))?;
        acc.checked_add(span)
            .ok_or_else(|| anyhow!("補修対象ブロック数の合計がオーバーフローしました"))
    })?;

    println!(
        "トランザクション不整合 {} 件に対応する {} ブロックを再投入します。",
        mismatches.len(),
        total_blocks
    );

    fill_missing_blocks_and_transactions(client, provider, args, block_columns, tx_columns, &ranges)
        .await
}

async fn fill_missing_blocks_and_transactions(
    client: &Client,
    provider: &impl Provider,
    args: &Args,
    block_columns: &[ColumnInfo],
    tx_columns: &[ColumnInfo],
    missing_ranges: &[(u64, u64)],
) -> Result<()> {
    if missing_ranges.is_empty() {
        return Ok(());
    }

    let mut total_blocks = 0u64;
    let mut total_transactions = 0u64;

    for (start, end) in missing_ranges {
        for block_number in *start..=*end {
            info!("ブロック {} の補完を開始します。", block_number);
            let response = provider
                .get_block_by_number(BlockNumberOrTag::Number(block_number))
                .full()
                .await
                .with_context(|| format!("ブロック {} の取得に失敗しました", block_number))?
                .ok_or_else(|| {
                    anyhow!(
                        "Ethereumノードがブロック {} を返しませんでした",
                        block_number
                    )
                })?;

            if !response.transactions.is_full() {
                bail!(
                    "ブロック {} のトランザクション情報がハッシュのみで返却されました。",
                    block_number
                );
            }

            let block_json = serde_json::to_value(&response)
                .context("ブロックレスポンスをJSONへ変換できませんでした")?;
            let block_obj = block_json.as_object().ok_or_else(|| {
                anyhow!(
                    "ブロック {} のJSONがオブジェクトではありません",
                    block_number
                )
            })?;

            let tx_values = block_obj
                .get("transactions")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            let tx_entries: Vec<Map<String, Value>> = tx_values
                .into_iter()
                .filter_map(|value| match value {
                    Value::Object(map) => Some(map),
                    other => {
                        warn!(
                            "ブロック {} のトランザクション値がオブジェクトではありません: {:?}",
                            block_number, other
                        );
                        None
                    }
                })
                .collect();

            let tx_count = tx_entries.len();
            let block_row = build_block_row(block_columns, args, block_obj, block_number, tx_count)
                .with_context(|| {
                    format!("ブロック {} の行データ構築に失敗しました", block_number)
                })?;

            if block_row.is_empty() {
                warn!(
                    "ブロック {} に対応する挿入カラムが見つからず、INSERTをスキップします。",
                    block_number
                );
            } else {
                delete_block_row(client, args, block_number).await?;
                insert_row(client, &args.blocks_table, &block_row)
                    .await
                    .with_context(|| format!("ブロック {} の挿入に失敗しました", block_number))?;
            }

            delete_transactions_for_block(client, args, block_number).await?;

            let mut inserted_for_block = 0u64;
            for tx_map in tx_entries {
                let tx_row = build_transaction_row(tx_columns, args, &tx_map, block_number)
                    .with_context(|| {
                        format!(
                            "ブロック {} のトランザクション行構築に失敗しました",
                            block_number
                        )
                    })?;

                if tx_row.is_empty() {
                    let tx_hash = tx_map
                        .get("hash")
                        .and_then(Value::as_str)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "<unknown>".to_string());
                    warn!(
                        "トランザクション {} の挿入対象カラムがなく、スキップしました。",
                        tx_hash
                    );
                    continue;
                }

                insert_row(client, &args.transactions_table, &tx_row)
                    .await
                    .with_context(|| {
                        let tx_hash = tx_map
                            .get("hash")
                            .and_then(Value::as_str)
                            .unwrap_or("<unknown>");
                        format!(
                            "トランザクション {} (ブロック {}) の挿入に失敗しました",
                            tx_hash, block_number
                        )
                    })?;
                inserted_for_block += 1;
            }

            total_blocks += 1;
            total_transactions += inserted_for_block;

            println!(
                "ブロック {} を補完しました (トランザクション {} 件)。",
                block_number, inserted_for_block
            );
        }
    }

    println!(
        "補完完了: ブロック {} 件、トランザクション {} 件を挿入しました。",
        total_blocks, total_transactions
    );

    Ok(())
}

fn build_block_row(
    block_columns: &[ColumnInfo],
    args: &Args,
    block_obj: &Map<String, Value>,
    block_number: u64,
    tx_count: usize,
) -> Result<Vec<ColumnValue>> {
    let mut row = Vec::new();

    for column in block_columns {
        let type_info = parse_column_type(&column.column_type);
        let value = if column.name == args.blocks_number_column {
            Some(build_number_from_u64(&type_info, block_number)?)
        } else {
            match column.name.as_str() {
                "hash" => build_hex_value(&type_info, block_obj.get("hash"), "hash", "hash")?,
                "parent_hash" => build_hex_value(
                    &type_info,
                    block_obj.get("parentHash"),
                    "parentHash",
                    "parent_hash",
                )?,
                "miner" | "coinbase" => build_string_value(
                    &type_info,
                    block_obj.get("miner"),
                    "miner",
                    column.name.as_str(),
                )?,
                "nonce" => build_hex_value(
                    &type_info,
                    block_obj.get("nonce"),
                    "nonce",
                    column.name.as_str(),
                )?,
                "sha3_uncles" => build_hex_value(
                    &type_info,
                    block_obj.get("sha3Uncles"),
                    "sha3Uncles",
                    column.name.as_str(),
                )?,
                "logs_bloom" => build_hex_value(
                    &type_info,
                    block_obj.get("logsBloom"),
                    "logsBloom",
                    column.name.as_str(),
                )?,
                "transactions_root" => build_hex_value(
                    &type_info,
                    block_obj.get("transactionsRoot"),
                    "transactionsRoot",
                    column.name.as_str(),
                )?,
                "state_root" => build_hex_value(
                    &type_info,
                    block_obj.get("stateRoot"),
                    "stateRoot",
                    column.name.as_str(),
                )?,
                "receipts_root" => build_hex_value(
                    &type_info,
                    block_obj.get("receiptsRoot"),
                    "receiptsRoot",
                    column.name.as_str(),
                )?,
                "mix_hash" => build_hex_value(
                    &type_info,
                    block_obj.get("mixHash"),
                    "mixHash",
                    column.name.as_str(),
                )?,
                "base_fee_per_gas" => build_numeric_value(
                    &type_info,
                    block_obj.get("baseFeePerGas"),
                    "baseFeePerGas",
                    column.name.as_str(),
                )?,
                "blob_gas_used" => build_numeric_value(
                    &type_info,
                    block_obj.get("blobGasUsed"),
                    "blobGasUsed",
                    column.name.as_str(),
                )?,
                "blob_gas_price" => build_numeric_value(
                    &type_info,
                    block_obj.get("blobGasPrice"),
                    "blobGasPrice",
                    column.name.as_str(),
                )?,
                "difficulty" => build_numeric_value(
                    &type_info,
                    block_obj.get("difficulty"),
                    "difficulty",
                    column.name.as_str(),
                )?,
                "total_difficulty" => build_numeric_value(
                    &type_info,
                    block_obj.get("totalDifficulty"),
                    "totalDifficulty",
                    column.name.as_str(),
                )?,
                "size" => build_numeric_value(
                    &type_info,
                    block_obj.get("size"),
                    "size",
                    column.name.as_str(),
                )?,
                "gas_limit" => build_numeric_value(
                    &type_info,
                    block_obj.get("gasLimit"),
                    "gasLimit",
                    column.name.as_str(),
                )?,
                "gas_used" => build_numeric_value(
                    &type_info,
                    block_obj.get("gasUsed"),
                    "gasUsed",
                    column.name.as_str(),
                )?,
                "timestamp" => build_numeric_value(
                    &type_info,
                    block_obj.get("timestamp"),
                    "timestamp",
                    column.name.as_str(),
                )?,
                "extra_data" => build_hex_value(
                    &type_info,
                    block_obj.get("extraData"),
                    "extraData",
                    column.name.as_str(),
                )?,
                "withdrawals_root" => build_hex_value(
                    &type_info,
                    block_obj.get("withdrawalsRoot"),
                    "withdrawalsRoot",
                    column.name.as_str(),
                )?,
                "parent_beacon_block_root" => build_hex_value(
                    &type_info,
                    block_obj.get("parentBeaconBlockRoot"),
                    "parentBeaconBlockRoot",
                    column.name.as_str(),
                )?,
                "transactions_count" | "tx_count" => {
                    Some(build_number_from_u64(&type_info, tx_count as u64)?)
                }
                "withdrawals" => build_json_value(
                    &type_info,
                    block_obj.get("withdrawals"),
                    column.name.as_str(),
                )?,
                "uncles" => {
                    build_json_value(&type_info, block_obj.get("uncles"), column.name.as_str())?
                }
                _ => None,
            }
        };

        if let Some(value) = value {
            row.push(ColumnValue {
                name: column.name.clone(),
                value,
            });
        }
    }

    Ok(row)
}

fn build_transaction_row(
    tx_columns: &[ColumnInfo],
    args: &Args,
    tx_obj: &Map<String, Value>,
    block_number: u64,
) -> Result<Vec<ColumnValue>> {
    let mut row = Vec::new();

    for column in tx_columns {
        let type_info = parse_column_type(&column.column_type);
        let value = if column.name == args.transactions_block_column {
            Some(build_number_from_u64(&type_info, block_number)?)
        } else {
            match column.name.as_str() {
                "hash" | "tx_hash" => {
                    build_hex_value(&type_info, tx_obj.get("hash"), "hash", column.name.as_str())?
                }
                "block_hash" => build_hex_value(
                    &type_info,
                    tx_obj.get("blockHash"),
                    "blockHash",
                    column.name.as_str(),
                )?,
                "nonce" => build_numeric_value(
                    &type_info,
                    tx_obj.get("nonce"),
                    "nonce",
                    column.name.as_str(),
                )?,
                "transaction_index" | "tx_index" => build_numeric_value(
                    &type_info,
                    tx_obj.get("transactionIndex"),
                    "transactionIndex",
                    column.name.as_str(),
                )?,
                "from" | "from_address" => build_string_value(
                    &type_info,
                    tx_obj.get("from"),
                    "from",
                    column.name.as_str(),
                )?,
                "to" | "to_address" => {
                    build_string_value(&type_info, tx_obj.get("to"), "to", column.name.as_str())?
                }
                "value" => build_numeric_value(
                    &type_info,
                    tx_obj.get("value"),
                    "value",
                    column.name.as_str(),
                )?,
                "gas" => {
                    build_numeric_value(&type_info, tx_obj.get("gas"), "gas", column.name.as_str())?
                }
                "gas_price" => build_numeric_value(
                    &type_info,
                    tx_obj.get("gasPrice"),
                    "gasPrice",
                    column.name.as_str(),
                )?,
                "max_fee_per_gas" | "max_fee_per_gwei" => build_numeric_value(
                    &type_info,
                    tx_obj.get("maxFeePerGas"),
                    "maxFeePerGas",
                    column.name.as_str(),
                )?,
                "max_priority_fee_per_gas" | "priority_fee" => build_numeric_value(
                    &type_info,
                    tx_obj.get("maxPriorityFeePerGas"),
                    "maxPriorityFeePerGas",
                    column.name.as_str(),
                )?,
                "max_fee_per_blob_gas" => build_numeric_value(
                    &type_info,
                    tx_obj.get("maxFeePerBlobGas"),
                    "maxFeePerBlobGas",
                    column.name.as_str(),
                )?,
                "input" | "data" => build_hex_value(
                    &type_info,
                    tx_obj.get("input"),
                    "input",
                    column.name.as_str(),
                )?,
                "type" | "tx_type" => build_numeric_value(
                    &type_info,
                    tx_obj.get("type"),
                    "type",
                    column.name.as_str(),
                )?,
                "chain_id" => build_numeric_value(
                    &type_info,
                    tx_obj.get("chainId"),
                    "chainId",
                    column.name.as_str(),
                )?,
                "access_list" => {
                    build_json_value(&type_info, tx_obj.get("accessList"), column.name.as_str())?
                }
                "blob_versioned_hashes" => build_json_value(
                    &type_info,
                    tx_obj.get("blobVersionedHashes"),
                    column.name.as_str(),
                )?,
                "r" => build_numeric_value(&type_info, tx_obj.get("r"), "r", column.name.as_str())?,
                "s" => build_numeric_value(&type_info, tx_obj.get("s"), "s", column.name.as_str())?,
                "v" => build_numeric_value(&type_info, tx_obj.get("v"), "v", column.name.as_str())?,
                "y_parity" => build_numeric_value(
                    &type_info,
                    tx_obj.get("yParity"),
                    "yParity",
                    column.name.as_str(),
                )?,
                "signature" => build_hex_value(
                    &type_info,
                    tx_obj.get("signature"),
                    "signature",
                    column.name.as_str(),
                )?,
                "l1_fee" => build_numeric_value(
                    &type_info,
                    tx_obj.get("l1Fee"),
                    "l1Fee",
                    column.name.as_str(),
                )?,
                _ => None,
            }
        };

        if let Some(value) = value {
            row.push(ColumnValue {
                name: column.name.clone(),
                value,
            });
        }
    }

    Ok(row)
}

async fn delete_block_row(client: &Client, args: &Args, block_number: u64) -> Result<()> {
    let result = client
        .query(&format!(
            "ALTER TABLE {} DELETE WHERE {} = ?",
            args.blocks_table, args.blocks_number_column
        ))
        .bind(block_number)
        .execute()
        .await;

    if let Err(err) = result {
        warn!(
            "ブロック {} の既存データ削除に失敗しました (継続します): {}",
            block_number, err
        );
    }

    Ok(())
}

async fn delete_transactions_for_block(
    client: &Client,
    args: &Args,
    block_number: u64,
) -> Result<()> {
    let result = client
        .query(&format!(
            "ALTER TABLE {} DELETE WHERE {} = ?",
            args.transactions_table, args.transactions_block_column
        ))
        .bind(block_number)
        .execute()
        .await;

    if let Err(err) = result {
        warn!(
            "ブロック {} の既存トランザクション削除に失敗しました (継続します): {}",
            block_number, err
        );
    }

    Ok(())
}

async fn insert_row(client: &Client, table: &str, row: &[ColumnValue]) -> Result<()> {
    if row.is_empty() {
        return Ok(());
    }

    let column_names = row
        .iter()
        .map(|col| col.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; row.len()].join(", ");

    let mut query = client.query(&format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table, column_names, placeholders
    ));

    for column in row {
        query = column.value.clone().bind(query);
    }

    query.execute().await?;
    Ok(())
}

fn parse_column_type(raw: &str) -> ColumnTypeInfo {
    let mut nullable = false;
    let mut ty = raw.trim();

    loop {
        if let Some(stripped) = ty.strip_prefix("Nullable(") {
            if let Some(inner) = stripped.strip_suffix(')') {
                nullable = true;
                ty = inner.trim();
                continue;
            }
        }
        if let Some(stripped) = ty.strip_prefix("LowCardinality(") {
            if let Some(inner) = stripped.strip_suffix(')') {
                ty = inner.trim();
                continue;
            }
        }
        if let Some(stripped) = ty.strip_prefix("SimpleAggregateFunction(") {
            if let Some(inner) = stripped.strip_suffix(')') {
                if let Some(pos) = inner.rfind(',') {
                    ty = inner[pos + 1..].trim();
                    continue;
                }
            }
        }
        break;
    }

    ColumnTypeInfo {
        base: parse_base_type(ty),
        nullable,
    }
}

fn parse_base_type(base: &str) -> BaseType {
    match base {
        "String" => BaseType::String,
        "Bool" | "Boolean" => BaseType::Bool,
        "DateTime" => BaseType::DateTime,
        "Float32" => BaseType::Float(32),
        "Float64" => BaseType::Float(64),
        "UUID" => BaseType::FixedString(16),
        _ if base.starts_with("FixedString(") && base.ends_with(')') => {
            let inner = &base["FixedString(".len()..base.len() - 1];
            let len = inner.parse::<usize>().unwrap_or(0);
            BaseType::FixedString(len)
        }
        _ if base == "Array(String)" => BaseType::ArrayString,
        _ if base.starts_with("DateTime64(") && base.ends_with(')') => {
            let inner = &base["DateTime64(".len()..base.len() - 1];
            let scale = inner.parse::<u32>().unwrap_or(0);
            BaseType::DateTime64(scale)
        }
        _ if base.starts_with("Decimal(") && base.ends_with(')') => {
            let inner = &base["Decimal(".len()..base.len() - 1];
            let mut parts = inner.split(',').map(|p| p.trim());
            if let (Some(precision), Some(scale)) = (parts.next(), parts.next()) {
                if let (Ok(p), Ok(s)) = (precision.parse::<u32>(), scale.parse::<u32>()) {
                    return BaseType::Decimal {
                        precision: p,
                        scale: s,
                    };
                }
            }
            BaseType::Decimal {
                precision: 0,
                scale: 0,
            }
        }
        _ if base.starts_with("Decimal32(") && base.ends_with(')') => {
            let inner = &base["Decimal32(".len()..base.len() - 1];
            let scale = inner.parse::<u32>().unwrap_or(0);
            BaseType::Decimal {
                precision: 9,
                scale,
            }
        }
        _ if base.starts_with("Decimal64(") && base.ends_with(')') => {
            let inner = &base["Decimal64(".len()..base.len() - 1];
            let scale = inner.parse::<u32>().unwrap_or(0);
            BaseType::Decimal {
                precision: 18,
                scale,
            }
        }
        _ if base.starts_with("Decimal128(") && base.ends_with(')') => {
            let inner = &base["Decimal128(".len()..base.len() - 1];
            let scale = inner.parse::<u32>().unwrap_or(0);
            BaseType::Decimal {
                precision: 38,
                scale,
            }
        }
        _ if base.starts_with("Decimal256(") && base.ends_with(')') => {
            let inner = &base["Decimal256(".len()..base.len() - 1];
            let scale = inner.parse::<u32>().unwrap_or(0);
            BaseType::Decimal {
                precision: 76,
                scale,
            }
        }
        _ if base.starts_with("Enum8(") || base.starts_with("Enum16(") => BaseType::String,
        "UInt8" => BaseType::UInt(8),
        "UInt16" => BaseType::UInt(16),
        "UInt32" => BaseType::UInt(32),
        "UInt64" => BaseType::UInt(64),
        "UInt128" => BaseType::UInt(128),
        "UInt256" => BaseType::UInt(256),
        "Int8" => BaseType::Int(8),
        "Int16" => BaseType::Int(16),
        "Int32" => BaseType::Int(32),
        "Int64" => BaseType::Int(64),
        "Int128" => BaseType::Int(128),
        "Int256" => BaseType::Int(256),
        other => BaseType::Unknown(other.to_string()),
    }
}

fn decode_hex_bytes(data: &str) -> Result<Vec<u8>> {
    let trimmed = data.trim();
    let without_prefix = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    if without_prefix.is_empty() {
        return Ok(Vec::new());
    }
    let normalized = if without_prefix.len().is_multiple_of(2) {
        without_prefix.to_owned()
    } else {
        format!("0{}", without_prefix)
    };
    hex::decode(normalized).map_err(|err| anyhow!("16進数データのデコードに失敗しました: {}", err))
}

fn parse_hex_to_u256(value_opt: Option<&Value>, field_name: &str) -> Result<Option<U256>> {
    match value_opt {
        Some(Value::String(text)) => {
            if text.is_empty() || text == "0x" {
                return Ok(Some(U256::ZERO));
            }
            let trimmed = text.trim_start_matches("0x");
            let normalized = if trimmed.is_empty() { "0" } else { trimmed };
            U256::from_str_radix(normalized, 16)
                .map(Some)
                .map_err(|err| {
                    anyhow!(
                        "フィールド `{}` を U256 に変換できませんでした: {}",
                        field_name,
                        err
                    )
                })
        }
        Some(Value::Null) | None => Ok(None),
        other => bail!(
            "フィールド `{}` の値が文字列ではありません: {:?}",
            field_name,
            other
        ),
    }
}

fn build_numeric_value(
    type_info: &ColumnTypeInfo,
    value_opt: Option<&Value>,
    field_name: &str,
    column_name: &str,
) -> Result<Option<QueryValue>> {
    let parsed = parse_hex_to_u256(value_opt, field_name)?;
    let convert = |value: U256| -> Result<QueryValue> {
        match type_info.base {
            BaseType::DateTime => assign_datetime_value(type_info, value, column_name),
            BaseType::DateTime64(scale) => {
                assign_datetime64_value(type_info, value, scale, column_name)
            }
            _ => assign_u256_generic(type_info, value, column_name),
        }
    };

    match parsed {
        Some(value) => Ok(Some(convert(value)?)),
        None => {
            if type_info.nullable {
                Ok(Some(null_query_value(type_info)))
            } else {
                Ok(Some(convert(U256::ZERO)?))
            }
        }
    }
}

fn assign_datetime_value(
    type_info: &ColumnTypeInfo,
    value: U256,
    column_name: &str,
) -> Result<QueryValue> {
    let seconds: u64 = value.try_into().map_err(|_| {
        anyhow!(
            "カラム `{}` に格納するタイムスタンプが u64 を超えています",
            column_name
        )
    })?;
    build_number_from_u64(type_info, seconds)
}

fn assign_datetime64_value(
    type_info: &ColumnTypeInfo,
    value: U256,
    scale: u32,
    column_name: &str,
) -> Result<QueryValue> {
    let seconds: u64 = value.try_into().map_err(|_| {
        anyhow!(
            "カラム `{}` に格納するタイムスタンプが u64 を超えています",
            column_name
        )
    })?;
    let multiplier = 10u64.checked_pow(scale).ok_or_else(|| {
        anyhow!(
            "DateTime64 カラム `{}` のスケール {} が大きすぎます",
            column_name,
            scale
        )
    })?;
    let scaled = seconds.checked_mul(multiplier).ok_or_else(|| {
        anyhow!(
            "DateTime64 カラム `{}` への値変換でオーバーフローしました",
            column_name
        )
    })?;
    build_number_from_u64(type_info, scaled)
}

fn assign_u256_generic(
    type_info: &ColumnTypeInfo,
    value: U256,
    column_name: &str,
) -> Result<QueryValue> {
    match type_info.base {
        BaseType::UInt(bits) if bits <= 64 => {
            let val: u64 = value.try_into().map_err(|_| {
                anyhow!(
                    "カラム `{}` に格納する値が {}bit の範囲を超えています",
                    column_name,
                    bits
                )
            })?;
            if type_info.nullable {
                Ok(QueryValue::NullableUInt64(Some(val)))
            } else {
                Ok(QueryValue::UInt64(val))
            }
        }
        BaseType::Float(_) => {
            let decimal = value.to_string();
            let float_val: f64 = decimal.parse().map_err(|err| {
                anyhow!(
                    "カラム `{}` の値を f64 に変換できませんでした: {}",
                    column_name,
                    err
                )
            })?;
            if type_info.nullable {
                Ok(QueryValue::NullableFloat64(Some(float_val)))
            } else {
                Ok(QueryValue::Float64(float_val))
            }
        }
        BaseType::Bool => {
            let is_true = !value.is_zero();
            if type_info.nullable {
                Ok(QueryValue::NullableBool(Some(is_true)))
            } else {
                Ok(QueryValue::Bool(is_true))
            }
        }
        BaseType::Int(bits) if bits <= 64 => {
            let val: u64 = value.try_into().map_err(|_| {
                anyhow!(
                    "カラム `{}` に格納する値が変換できませんでした",
                    column_name
                )
            })?;
            if val > i64::MAX as u64 {
                bail!("カラム `{}` に格納する値が i64 を超えています", column_name);
            }
            let signed = val as i64;
            if type_info.nullable {
                Ok(QueryValue::NullableInt64(Some(signed)))
            } else {
                Ok(QueryValue::Int64(signed))
            }
        }
        _ => {
            let decimal = value.to_string();
            if type_info.nullable {
                Ok(QueryValue::NullableString(Some(decimal)))
            } else {
                Ok(QueryValue::String(decimal))
            }
        }
    }
}

fn build_hex_value(
    type_info: &ColumnTypeInfo,
    value_opt: Option<&Value>,
    field_name: &str,
    column_name: &str,
) -> Result<Option<QueryValue>> {
    match type_info.base {
        BaseType::String | BaseType::Unknown(_) => match value_opt {
            Some(Value::String(text)) => {
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableString(Some(text.clone()))))
                } else {
                    Ok(Some(QueryValue::String(text.clone())))
                }
            }
            Some(Value::Null) | None => {
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableString(None)))
                } else {
                    Ok(Some(QueryValue::String("0x0".to_string())))
                }
            }
            other => bail!(
                "フィールド `{}` の値が文字列ではありません: {:?}",
                field_name,
                other
            ),
        },
        BaseType::FixedString(_) => match value_opt {
            Some(Value::String(text)) => {
                let bytes = decode_hex_bytes(text)?;
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableBytes(Some(bytes))))
                } else {
                    Ok(Some(QueryValue::Bytes(bytes)))
                }
            }
            Some(Value::Null) | None => {
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableBytes(None)))
                } else {
                    Ok(Some(QueryValue::Bytes(Vec::new())))
                }
            }
            other => bail!(
                "フィールド `{}` の値が文字列ではありません: {:?}",
                field_name,
                other
            ),
        },
        _ => build_numeric_value(type_info, value_opt, field_name, column_name),
    }
}

fn build_string_value(
    type_info: &ColumnTypeInfo,
    value_opt: Option<&Value>,
    field_name: &str,
    column_name: &str,
) -> Result<Option<QueryValue>> {
    match value_opt {
        Some(Value::String(text)) => {
            if type_info.nullable {
                Ok(Some(QueryValue::NullableString(Some(text.clone()))))
            } else {
                Ok(Some(QueryValue::String(text.clone())))
            }
        }
        Some(Value::Null) | None => {
            if type_info.nullable {
                Ok(Some(QueryValue::NullableString(None)))
            } else {
                bail!(
                    "フィールド `{}` の値が存在せず、カラム `{}` は Nullable ではありません",
                    field_name,
                    column_name
                );
            }
        }
        other => bail!(
            "フィールド `{}` の値が文字列ではありません: {:?}",
            field_name,
            other
        ),
    }
}

fn build_json_value(
    type_info: &ColumnTypeInfo,
    value_opt: Option<&Value>,
    column_name: &str,
) -> Result<Option<QueryValue>> {
    match type_info.base {
        BaseType::ArrayString => match value_opt {
            None | Some(Value::Null) => {
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableArrayString(None)))
                } else {
                    Ok(Some(QueryValue::ArrayString(Vec::new())))
                }
            }
            Some(Value::Array(items)) => {
                let mut vec = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Value::String(text) => vec.push(text.clone()),
                        other => {
                            bail!(
                                "カラム `{}` に配列以外の要素が含まれています: {:?}",
                                column_name,
                                other
                            );
                        }
                    }
                }

                if type_info.nullable {
                    Ok(Some(QueryValue::NullableArrayString(Some(vec))))
                } else {
                    Ok(Some(QueryValue::ArrayString(vec)))
                }
            }
            Some(Value::String(text)) => {
                let parsed: Vec<String> = serde_json::from_str(text).with_context(|| {
                    format!(
                        "カラム `{}` の文字列を配列として解釈できませんでした",
                        column_name
                    )
                })?;
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableArrayString(Some(parsed))))
                } else {
                    Ok(Some(QueryValue::ArrayString(parsed)))
                }
            }
            Some(other) => bail!(
                "カラム `{}` に対応する値が配列ではありません: {:?}",
                column_name,
                other
            ),
        },
        _ => match value_opt {
            None | Some(Value::Null) => {
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableString(None)))
                } else {
                    bail!(
                        "カラム `{}` の値が存在しませんが Nullable ではありません",
                        column_name
                    );
                }
            }
            Some(value) => {
                let serialized = serde_json::to_string(value).with_context(|| {
                    format!("カラム `{}` 用のJSON変換に失敗しました", column_name)
                })?;
                if type_info.nullable {
                    Ok(Some(QueryValue::NullableString(Some(serialized))))
                } else {
                    Ok(Some(QueryValue::String(serialized)))
                }
            }
        },
    }
}

fn build_number_from_u64(type_info: &ColumnTypeInfo, value: u64) -> Result<QueryValue> {
    match type_info.base {
        BaseType::UInt(bits) if bits <= 64 => {
            if type_info.nullable {
                Ok(QueryValue::NullableUInt64(Some(value)))
            } else {
                Ok(QueryValue::UInt64(value))
            }
        }
        BaseType::Int(bits) if bits <= 64 => {
            if value > i64::MAX as u64 {
                bail!("値が有効範囲を超えています");
            }
            let signed = value as i64;
            if type_info.nullable {
                Ok(QueryValue::NullableInt64(Some(signed)))
            } else {
                Ok(QueryValue::Int64(signed))
            }
        }
        BaseType::Float(_) => {
            let as_f64 = value as f64;
            if type_info.nullable {
                Ok(QueryValue::NullableFloat64(Some(as_f64)))
            } else {
                Ok(QueryValue::Float64(as_f64))
            }
        }
        _ => {
            let text = value.to_string();
            if type_info.nullable {
                Ok(QueryValue::NullableString(Some(text)))
            } else {
                Ok(QueryValue::String(text))
            }
        }
    }
}

fn null_query_value(type_info: &ColumnTypeInfo) -> QueryValue {
    match type_info.base {
        BaseType::UInt(_) | BaseType::DateTime | BaseType::DateTime64(_) => {
            QueryValue::NullableUInt64(None)
        }
        BaseType::Int(_) => QueryValue::NullableInt64(None),
        BaseType::Float(_) => QueryValue::NullableFloat64(None),
        BaseType::Bool => QueryValue::NullableBool(None),
        BaseType::FixedString(_) => QueryValue::NullableBytes(None),
        BaseType::ArrayString => QueryValue::NullableArrayString(None),
        _ => QueryValue::NullableString(None),
    }
}

async fn print_table_schema(client: &Client, table: &str, label: &str) -> Result<Vec<ColumnInfo>> {
    let columns = fetch_table_columns(client, table).await?;
    if columns.is_empty() {
        println!(
            "Table `{}` ({}) has no columns or does not exist.",
            table, label
        );
    } else {
        println!("Table `{}` ({}) columns:", table, label);
        for column in &columns {
            println!("  {} {}", column.name, column.column_type);
        }
    }
    Ok(columns)
}

async fn report_unfinished_mutations(client: &Client) -> Result<()> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct MutationRow {
        table: String,
        pending_mutations: u64,
    }

    info!("Querying unfinished mutations from ClickHouse.");
    let query = "SELECT \
            table, \
            count() AS pending_mutations \
        FROM system.mutations \
        WHERE database = currentDatabase() \
          AND is_done = 0 \
        GROUP BY table \
        ORDER BY table";

    let rows: Vec<MutationRow> = client
        .query(query)
        .fetch_all()
        .await
        .context("Failed to query system.mutations for unfinished mutations")?;

    if rows.is_empty() {
        println!("No unfinished mutations found in the current ClickHouse database.");
        return Ok(());
    }

    let total_pending: u64 = rows.iter().map(|row| row.pending_mutations).sum();
    println!(
        "Found {} unfinished mutation(s) across {} table(s):",
        total_pending,
        rows.len()
    );
    for row in rows {
        println!("  {}: {}", row.table, row.pending_mutations);
    }

    Ok(())
}

fn ensure_column_exists(
    columns: &[ColumnInfo],
    column_name: &str,
    table_name: &str,
    arg_hint: &str,
) -> Result<()> {
    let exists = columns.iter().any(|col| col.name == column_name);
    if exists {
        Ok(())
    } else {
        bail!(
            "Column `{}` not found in table `{}`. Specify the correct column with {}.",
            column_name,
            table_name,
            arg_hint
        );
    }
}

fn build_clickhouse_url(address: &str, port: u16) -> String {
    let trimmed = address.trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        format!("{trimmed}:{port}")
    } else {
        format!("http://{trimmed}:{port}")
    }
}

fn init_logging() {
    use env_logger::Env;

    if let Err(err) = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .try_init()
    {
        eprintln!("Failed to initialize logger: {err}");
    }
}

fn prompt_check_selection() -> Result<SelectedChecks> {
    println!("Select checks to run:");
    println!("  1) Block gap detection");
    println!("  2) Transaction mismatch detection");
    println!("  3) Duplicate block detection");
    println!("  4) Transaction table gap detection");
    println!("  5) Unfinished mutation count");
    println!("Enter numbers separated by commas (e.g. `1,3`) or press Enter for all:");
    print!("> ");
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    let read = io::stdin()
        .read_line(&mut input)
        .context("Failed to read selection from stdin")?;

    let mut selection = SelectedChecks {
        block_gap: false,
        tx_gap: false,
        tx_mismatch: false,
        duplicates: false,
        mutations: false,
    };

    if read == 0 || input.trim().is_empty() {
        selection.block_gap = true;
        selection.tx_gap = true;
        selection.tx_mismatch = true;
        selection.duplicates = true;
        selection.mutations = true;
        return Ok(selection);
    }

    for token in input.split(',') {
        let trimmed = token.trim().to_lowercase();
        if trimmed.is_empty() {
            continue;
        }
        match trimmed.as_str() {
            "1" | "block" | "block_gap" | "blocks" | "gap" => {
                selection.block_gap = true;
            }
            "2" | "tx" | "transactions" | "transaction" | "tx_mismatch" => {
                selection.tx_mismatch = true;
            }
            "3" | "dup" | "duplicate" | "duplicates" => {
                selection.duplicates = true;
            }
            "4" | "tx_gap" | "transaction_gap" | "transactions_gap" | "transactions_block_gap" => {
                selection.tx_gap = true;
            }
            "5" | "mutation" | "mutations" | "unfinished" | "unfinished_mutations" => {
                selection.mutations = true;
            }
            "all" | "a" => {
                selection.block_gap = true;
                selection.tx_gap = true;
                selection.tx_mismatch = true;
                selection.duplicates = true;
                selection.mutations = true;
            }
            other => {
                bail!("Unknown selection: `{}`", other);
            }
        }
    }

    if !selection.block_gap
        && !selection.tx_gap
        && !selection.tx_mismatch
        && !selection.duplicates
        && !selection.mutations
    {
        bail!("No checks selected.");
    }

    Ok(selection)
}

async fn fetch_table_metadata(client: &Client, table: &str) -> Result<TableMetadata> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TableRow {
        engine: String,
        engine_full: String,
    }

    let query = format!(
        "SELECT engine, engine_full \
         FROM system.tables \
         WHERE database = currentDatabase() \
           AND name = '{table}' \
         LIMIT 1",
        table = table
    );

    let rows: Vec<TableRow> = client.query(&query).fetch_all().await?;
    let (engine, engine_full) = if let Some(meta) = rows.into_iter().next() {
        let engine = if meta.engine.is_empty() {
            None
        } else {
            Some(meta.engine)
        };
        let engine_full = if meta.engine_full.is_empty() {
            None
        } else {
            Some(meta.engine_full)
        };
        (engine, engine_full)
    } else {
        (None, None)
    };

    let apply_final = engine
        .as_deref()
        .map(engine_requires_final)
        .unwrap_or(false)
        || engine_full
            .as_deref()
            .map(engine_requires_final)
            .unwrap_or(false);

    Ok(TableMetadata {
        engine,
        apply_final,
    })
}

fn engine_requires_final(engine: &str) -> bool {
    const FINAL_KEYWORDS: [&str; 5] = [
        "ReplacingMergeTree",
        "CollapsingMergeTree",
        "VersionedCollapsingMergeTree",
        "SummingMergeTree",
        "AggregatingMergeTree",
    ];
    FINAL_KEYWORDS
        .iter()
        .any(|pattern| engine.contains(pattern))
}

fn final_clause(use_final: bool) -> &'static str {
    if use_final {
        " FINAL"
    } else {
        ""
    }
}
