use alloy::eips::BlockNumberOrTag;
use alloy::providers::{Provider, ProviderBuilder};
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use clickhouse::Client;
use log::info;
use reqwest::Url;
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

    let provider = if checks.tx_mismatch {
        let eth_url = Url::parse(&args.eth_node_url)
            .with_context(|| format!("Invalid ETH node URL: {}", args.eth_node_url))?;
        Some(ProviderBuilder::default().connect_http(eth_url))
    } else {
        None
    };

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
        }
    }

    if checks.tx_gap {
        match fetch_table_stats(
            &client,
            &args.transactions_table,
            &args.transactions_block_column,
            tx_meta.apply_final,
        )
        .await?
        {
            None => println!(
                "No transactions found in table `{}`. Skipping transaction gap check.",
                args.transactions_table
            ),
            Some(tx_stats) => {
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
                    println!(
                        "No gaps detected between {} and {} in transactions table `{}`.",
                        tx_stats.min, tx_stats.max, args.transactions_table
                    );
                } else {
                    let missing_ranges = find_missing_ranges(
                        &client,
                        &args.transactions_table,
                        &args.transactions_block_column,
                        tx_meta.apply_final,
                    )
                    .await?;
                    let context = format!("transactions table `{}`", args.transactions_table);
                    report_missing_ranges(&missing_ranges, &context);
                }
            }
        }
    }

    if checks.tx_mismatch {
        let provider = provider
            .as_ref()
            .expect("provider must exist when tx_mismatch is selected");
        let mismatches = find_transaction_mismatches(
            &client,
            &args,
            provider,
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
            for mismatch in mismatches {
                println!(
                    "  block {}: node={} clickhouse={}",
                    mismatch.block_number, mismatch.node_tx_count, mismatch.clickhouse_tx_count
                );
            }
        }
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
            assumeNotNull(prev_block) + 1 AS gap_start, \
            current_block - 1 AS gap_end \
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

    let rows: Vec<GapRow> = client.query(&query).fetch_all().await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.gap_start, row.gap_end))
        .collect())
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

            if processed_blocks % 100 == 0 {
                info!(
                    "Checked transaction counts for {} blocks...",
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
    };

    if read == 0 || input.trim().is_empty() {
        selection.block_gap = true;
        selection.tx_gap = true;
        selection.tx_mismatch = true;
        selection.duplicates = true;
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
            "all" | "a" => {
                selection.block_gap = true;
                selection.tx_gap = true;
                selection.tx_mismatch = true;
                selection.duplicates = true;
            }
            other => {
                bail!("Unknown selection: `{}`", other);
            }
        }
    }

    if !selection.block_gap && !selection.tx_gap && !selection.tx_mismatch && !selection.duplicates
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
