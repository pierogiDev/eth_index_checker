use alloy::consensus::{
    BlockHeader, Transaction as ConsensusTransactionTrait, TxEnvelope, Typed2718,
};
use alloy::eips::BlockNumberOrTag;
use alloy::network::TransactionResponse;
use alloy::primitives::{Signature, TxKind, B256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::eth::{Block as RpcBlock, SyncStatus, Transaction as RpcTransaction};
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use clickhouse::Client;
use log::info;
use reqwest::Url;
use serde_json::json;
use std::io::{self, Write};

const DEFAULT_CLICKHOUSE_ADDRESS: &str = env!("CLICKHOUSE_ADDRESS");
const DEFAULT_CLICKHOUSE_PORT: &str = env!("CLICKHOUSE_PORT");
const DEFAULT_CLICKHOUSE_USER: &str = env!("CLICKHOUSE_USER");
const DEFAULT_CLICKHOUSE_PASSWORD: &str = env!("CLICKHOUSE_PASSWORD");
const DEFAULT_CLICKHOUSE_DATABASE: &str = env!("CLICKHOUSE_DATABASE");
const DEFAULT_ETH_NODE_URL: &str = env!("ETH_NODE_URL");
const TX_CHUNK_SIZE: usize = 10_000;
const DUPLICATE_SAMPLE_LIMIT: usize = 20;
const INSERT_BATCH_SIZE: usize = 256;

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
    /// Column name that stores the transaction hash in the transactions table
    #[arg(long, env = "TRANSACTIONS_HASH_COLUMN", default_value = "hash")]
    transactions_hash_column: String,
    /// Receipts table name
    #[arg(long, env = "RECEIPTS_TABLE", default_value = "receipts")]
    receipts_table: String,
    /// Column name that stores the block number in the receipts table
    #[arg(long, env = "RECEIPTS_BLOCK_COLUMN", default_value = "block_number")]
    receipts_block_column: String,
    /// Column name that stores the transaction hash in the receipts table
    #[arg(
        long,
        env = "RECEIPTS_HASH_COLUMN",
        default_value = "transaction_hash"
    )]
    receipts_hash_column: String,
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

#[derive(Debug)]
struct HashDuplicateReport {
    total_extra_rows: u64,
    samples: Vec<HashDuplicateEntry>,
    truncated: bool,
}

#[derive(Debug)]
struct HashDuplicateEntry {
    hash_value: String,
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
    receipts_gap: bool,
    tx_mismatch: bool,
    duplicates: bool,
    tx_duplicates: bool,
    receipts_duplicates: bool,
    mutations: bool,
    optimize_blocks: bool,
    optimize_transactions: bool,
    sync_status: bool,
    show_schema: bool,
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

    let block_columns = fetch_table_columns(&client, &args.blocks_table).await?;
    let tx_columns = fetch_table_columns(&client, &args.transactions_table).await?;
    let needs_receipts_columns =
        checks.receipts_gap || checks.receipts_duplicates || checks.show_schema;
    let receipts_columns = if needs_receipts_columns {
        fetch_table_columns(&client, &args.receipts_table).await?
    } else {
        Vec::new()
    };

    if checks.show_schema {
        print_columns(&block_columns, &args.blocks_table, "blocks");
        print_columns(&tx_columns, &args.transactions_table, "transactions");
        print_columns(&receipts_columns, &args.receipts_table, "receipts");
    }

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

    let receipts_meta = if checks.receipts_gap || checks.receipts_duplicates {
        let meta = fetch_table_metadata(&client, &args.receipts_table).await?;
        if meta.apply_final {
            if let Some(engine) = &meta.engine {
                println!(
                    "Table `{}` uses engine `{}`; applying FINAL for consistency.",
                    args.receipts_table, engine
                );
            } else {
                println!(
                    "Table `{}` requires FINAL for consistent reads; applying FINAL.",
                    args.receipts_table
                );
            }
        }
        Some(meta)
    } else {
        None
    };

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
    ensure_column_exists(
        &tx_columns,
        &args.transactions_hash_column,
        &args.transactions_table,
        "--transactions-hash-column",
    )?;
    if checks.receipts_gap {
        ensure_column_exists(
            &receipts_columns,
            &args.receipts_block_column,
            &args.receipts_table,
            "--receipts-block-column",
        )?;
    }
    if checks.receipts_duplicates {
        ensure_column_exists(
            &receipts_columns,
            &args.receipts_hash_column,
            &args.receipts_table,
            "--receipts-hash-column",
        )?;
    }

    if checks.mutations {
        report_unfinished_mutations(&client).await?;
    }

    let mut provider: Option<RootProvider> = None;

    if checks.sync_status {
        let provider_ref = ensure_provider(&mut provider, &args.eth_node_url)?;
        report_sync_status(provider_ref).await?;
    }

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
        let duplicate_rows = block_stats
            .total_rows
            .saturating_sub(block_stats.distinct_count);
        println!("Warning: found {} duplicate block rows.", duplicate_rows);
        if let Some(duplicates) = find_duplicate_blocks(
            &client,
            &args,
            blocks_meta.apply_final,
            block_stats.total_rows,
            duplicate_rows,
        )
        .await?
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

    if checks.tx_duplicates {
        match fetch_hash_column_stats(
            &client,
            &args.transactions_table,
            &args.transactions_hash_column,
            tx_meta.apply_final,
        )
        .await?
        {
            None => println!(
                "No transactions found in table `{}`.",
                args.transactions_table
            ),
            Some((total_rows, distinct_values)) => {
                let duplicate_rows = total_rows.saturating_sub(distinct_values);
                if duplicate_rows == 0 {
                    println!(
                        "No duplicate transaction hashes found in table `{}`.",
                        args.transactions_table
                    );
                } else if let Some(duplicates) = find_duplicate_hashes(
                    &client,
                    &args.transactions_table,
                    &args.transactions_hash_column,
                    tx_meta.apply_final,
                    total_rows,
                    duplicate_rows,
                    "transaction hash",
                )
                .await?
                {
                    println!(
                        "Detected {} duplicate transaction occurrence(s) across {} hash(es):",
                        duplicates.total_extra_rows,
                        duplicates.samples.len()
                    );
                    for entry in &duplicates.samples {
                        println!(
                            "  tx {} appears {} times",
                            entry.hash_value, entry.occurrences
                        );
                    }
                    if duplicates.truncated {
                        println!(
                            "  ...and more (showing first {} transaction hash(es)).",
                            DUPLICATE_SAMPLE_LIMIT
                        );
                    }
                }
            }
        }
    }

    if checks.receipts_duplicates {
        let receipts_apply_final = receipts_meta
            .as_ref()
            .expect("receipts_meta must exist when receipts checks run")
            .apply_final;
        match fetch_hash_column_stats(
            &client,
            &args.receipts_table,
            &args.receipts_hash_column,
            receipts_apply_final,
        )
        .await?
        {
            None => println!("No receipts found in table `{}`.", args.receipts_table),
            Some((total_rows, distinct_values)) => {
                let duplicate_rows = total_rows.saturating_sub(distinct_values);
                if duplicate_rows == 0 {
                    println!(
                        "No duplicate receipt hashes found in table `{}`.",
                        args.receipts_table
                    );
                } else if let Some(duplicates) = find_duplicate_hashes(
                    &client,
                    &args.receipts_table,
                    &args.receipts_hash_column,
                    receipts_apply_final,
                    total_rows,
                    duplicate_rows,
                    "receipt hash",
                )
                .await?
                {
                    println!(
                        "Detected {} duplicate receipt occurrence(s) across {} hash(es):",
                        duplicates.total_extra_rows,
                        duplicates.samples.len()
                    );
                    for entry in &duplicates.samples {
                        println!(
                            "  receipt {} appears {} times",
                            entry.hash_value, entry.occurrences
                        );
                    }
                    if duplicates.truncated {
                        println!(
                            "  ...and more (showing first {} receipt hash(es)).",
                            DUPLICATE_SAMPLE_LIMIT
                        );
                    }
                }
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
                    println!("Fetching missing blocks from the Ethereum node and backfilling ClickHouse.");
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
                    println!("Skipped backfilling missing data based on user choice.");
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

    if checks.receipts_gap {
        let receipts_apply_final = receipts_meta
            .as_ref()
            .expect("receipts_meta must exist when receipts checks run")
            .apply_final;
        info!(
            "Starting receipts table gap detection for `{}` (block column = `{}`).",
            args.receipts_table, args.receipts_block_column
        );
        match fetch_table_stats(
            &client,
            &args.receipts_table,
            &args.receipts_block_column,
            receipts_apply_final,
        )
        .await?
        {
            None => {
                info!(
                    "Receipts table `{}` returned no rows; skipping gap detection.",
                    args.receipts_table
                );
                println!(
                    "No receipts found in table `{}`. Skipping receipts gap check.",
                    args.receipts_table
                );
            }
            Some(receipts_stats) => {
                info!(
                    "Loaded receipts stats: range {}…{}, {} distinct block number(s), {} total row(s).",
                    receipts_stats.min,
                    receipts_stats.max,
                    receipts_stats.distinct_count,
                    receipts_stats.total_rows
                );
                println!(
                    "Receipts block range: {}…{} ({} distinct block numbers, {} total rows)",
                    receipts_stats.min,
                    receipts_stats.max,
                    receipts_stats.distinct_count,
                    receipts_stats.total_rows
                );
                let span = receipts_stats
                    .max
                    .checked_sub(receipts_stats.min)
                    .context("Receipts block number range is invalid")?;
                let expected_blocks = span
                    .checked_add(1)
                    .context("Receipts block number range overflow")?;
                if expected_blocks == receipts_stats.distinct_count {
                    info!(
                        "No receipts block gaps detected between {} and {} in `{}`.",
                        receipts_stats.min, receipts_stats.max, args.receipts_table
                    );
                    println!(
                        "No gaps detected between {} and {} in receipts table `{}`.",
                        receipts_stats.min, receipts_stats.max, args.receipts_table
                    );
                } else {
                    info!(
                        "Scanning `{}` for missing receipts block ranges...",
                        args.receipts_table
                    );
                    let missing_ranges = find_missing_ranges(
                        &client,
                        &args.receipts_table,
                        &args.receipts_block_column,
                        receipts_apply_final,
                    )
                    .await?;
                    let missing_total: u64 = missing_ranges
                        .iter()
                        .map(|(start, end)| if end < start { 0 } else { end - start + 1 })
                        .sum();
                    info!(
                        "Detected {} missing receipts block(s) across {} gap(s).",
                        missing_total,
                        missing_ranges.len()
                    );
                    let context = format!("receipts table `{}`", args.receipts_table);
                    report_missing_ranges(&missing_ranges, &context);
                    info!(
                        "Finished receipts gap detection for `{}`.",
                        args.receipts_table
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
                println!("Repairing mismatched blocks using data from the Ethereum node.");
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
                println!("Skipped repairing transaction mismatches based on user choice.");
            }
        }
    }

    if checks.mutations {
        cleanup_mutations(&client, &args).await?;
    }

    if checks.optimize_blocks {
        optimize_blocks_table(&client, &args).await?;
    }

    if checks.optimize_transactions {
        optimize_transactions_table(&client, &args).await?;
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
    ensure_identifier(&args.transactions_hash_column, "transactions.hash column")?;
    ensure_identifier(&args.receipts_table, "receipts table name")?;
    ensure_identifier(
        &args.receipts_block_column,
        "receipts.block_number column",
    )?;
    ensure_identifier(&args.receipts_hash_column, "receipts.hash column")?;
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

const DUPLICATE_SCAN_CHUNK: usize = 4096;

async fn find_duplicate_blocks(
    client: &Client,
    args: &Args,
    use_final: bool,
    total_rows: u64,
    duplicate_rows: u64,
) -> Result<Option<BlockDuplicateReport>> {
    if duplicate_rows == 0 || total_rows <= 1 {
        return Ok(None);
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct BlockNumberRow {
        block_number: u64,
    }

    let final_clause = final_clause(use_final);
    let mut offset: u64 = 0;
    let mut last_value: Option<u64> = None;
    let mut run_length: u64 = 0;
    let mut samples = Vec::new();
    let mut truncated = false;

    while offset < total_rows && !truncated {
        if samples.len() >= DUPLICATE_SAMPLE_LIMIT {
            truncated = true;
            break;
        }

        let query = format!(
            "SELECT {col} AS block_number \
             FROM {table}{final_clause} \
             ORDER BY {col} \
             LIMIT {offset}, {limit}",
            col = args.blocks_number_column,
            table = args.blocks_table,
            final_clause = final_clause,
            offset = offset,
            limit = DUPLICATE_SCAN_CHUNK
        );

        let rows: Vec<BlockNumberRow> = client.query(&query).fetch_all().await?;
        if rows.is_empty() {
            break;
        }

        offset = offset.checked_add(rows.len() as u64).ok_or_else(|| {
            anyhow!("Overflow while computing offset during block table duplicate scan")
        })?;

        'row_scan: for row in rows {
            match last_value {
                Some(value) if value == row.block_number => {
                    run_length = run_length.saturating_add(1);
                }
                Some(value) => {
                    if run_length > 1 {
                        if samples.len() < DUPLICATE_SAMPLE_LIMIT {
                            samples.push(BlockDuplicateEntry {
                                block_number: value,
                                occurrences: run_length,
                            });
                        } else {
                            truncated = true;
                            break 'row_scan;
                        }
                    }
                    last_value = Some(row.block_number);
                    run_length = 1;
                }
                None => {
                    last_value = Some(row.block_number);
                    run_length = 1;
                }
            }
        }
    }

    if !truncated {
        if let Some(value) = last_value {
            if run_length > 1 {
                if samples.len() < DUPLICATE_SAMPLE_LIMIT {
                    samples.push(BlockDuplicateEntry {
                        block_number: value,
                        occurrences: run_length,
                    });
                } else {
                    truncated = true;
                }
            }
        }
    }

    if samples.is_empty() && duplicate_rows > 0 {
        truncated = true;
    }

    Ok(Some(BlockDuplicateReport {
        total_extra_rows: duplicate_rows,
        samples,
        truncated,
    }))
}

async fn find_duplicate_hashes(
    client: &Client,
    table: &str,
    column: &str,
    use_final: bool,
    total_rows: u64,
    duplicate_rows: u64,
    label: &str,
) -> Result<Option<HashDuplicateReport>> {
    if duplicate_rows == 0 || total_rows <= 1 {
        return Ok(None);
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TxHashRow {
        hash_value: String,
    }

    let final_clause = final_clause(use_final);
    let mut offset: u64 = 0;
    let mut last_value: Option<String> = None;
    let mut run_length: u64 = 0;
    let mut samples = Vec::new();
    let mut truncated = false;

    while offset < total_rows && !truncated {
        if samples.len() >= DUPLICATE_SAMPLE_LIMIT {
            truncated = true;
            break;
        }

        let query = format!(
            "SELECT toString({col}) AS hash_value \
             FROM {table}{final_clause} \
             ORDER BY {col} \
             LIMIT {offset}, {limit}",
            col = column,
            table = table,
            final_clause = final_clause,
            offset = offset,
            limit = DUPLICATE_SCAN_CHUNK
        );

        let rows: Vec<TxHashRow> = client.query(&query).fetch_all().await?;
        if rows.is_empty() {
            break;
        }

        offset = offset.checked_add(rows.len() as u64).ok_or_else(|| {
            anyhow!(
                "Overflow while computing offset during {label} duplicate scan"
            )
        })?;

        'row_scan: for row in rows {
            match &last_value {
                Some(value) if value == &row.hash_value => {
                    run_length = run_length.saturating_add(1);
                }
                Some(value) => {
                    if run_length > 1 {
                        if samples.len() < DUPLICATE_SAMPLE_LIMIT {
                            samples.push(HashDuplicateEntry {
                                hash_value: value.clone(),
                                occurrences: run_length,
                            });
                        } else {
                            truncated = true;
                            break 'row_scan;
                        }
                    }
                    last_value = Some(row.hash_value);
                    run_length = 1;
                }
                None => {
                    last_value = Some(row.hash_value);
                    run_length = 1;
                }
            }
        }
    }

    if !truncated && run_length > 1 {
        if samples.len() < DUPLICATE_SAMPLE_LIMIT {
            samples.push(HashDuplicateEntry {
                hash_value: last_value
                    .clone()
                    .expect("run_length > 1 implies last_value is Some"),
                occurrences: run_length,
            });
        } else {
            truncated = true;
        }
    }

    if samples.is_empty() && duplicate_rows > 0 {
        truncated = true;
    }

    Ok(Some(HashDuplicateReport {
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
        run_targeted_deletes(client, table, column).await?;
    }

    for table in tables {
        if prompt_yes_no(&format!("Run `OPTIMIZE TABLE {} FINAL` now?", table))? {
            optimize_table(client, &table).await?;
            println!(
                "  Skipped `OPTIMIZE TABLE {} FINAL` because read-only mode is enabled.",
                table
            );
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
    struct ColumnNameRow {
        name: String,
    }

    let columns_query = "\
        SELECT name \
        FROM system.columns \
        WHERE database = 'system' \
          AND table = 'mutations' \
          AND name IN ('parts_done', 'latest_failed_part_why')";

    let column_rows: Vec<ColumnNameRow> = client.query(columns_query).fetch_all().await?;

    // Older ClickHouse releases (< v26) lack `parts_done`/`latest_failed_part_why`; fall back to NULL.
    let has_parts_done = column_rows.iter().any(|row| row.name == "parts_done");
    let has_latest_failed_part_why = column_rows
        .iter()
        .any(|row| row.name == "latest_failed_part_why");

    let parts_done_select = if has_parts_done {
        "CAST(parts_done, 'Nullable(UInt64)') AS parts_done".to_string()
    } else {
        "CAST(NULL, 'Nullable(UInt64)') AS parts_done".to_string()
    };
    let latest_failed_part_why_select = if has_latest_failed_part_why {
        "latest_failed_part_why".to_string()
    } else {
        "CAST(NULL, 'Nullable(String)') AS latest_failed_part_why".to_string()
    };
    let latest_failed_part_select =
        "CAST(latest_failed_part, 'Nullable(String)') AS latest_failed_part";
    let parts_to_do_select = "CAST(parts_to_do, 'Nullable(UInt64)') AS parts_to_do";

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
            {latest_failed_part_select}, \
            {latest_failed_part_why_select}, \
            {parts_to_do_select}, \
            {parts_done_select} \
         FROM system.mutations \
         WHERE database = currentDatabase() \
           AND table IN ({table_filter}) \
           AND (is_done = 0 OR is_killed = 1 OR latest_failed_part != '') \
         ORDER BY create_time",
        table_filter = table_filter,
        parts_done_select = parts_done_select,
        latest_failed_part_why_select = latest_failed_part_why_select,
        latest_failed_part_select = latest_failed_part_select,
        parts_to_do_select = parts_to_do_select
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

async fn fetch_hash_column_stats(
    client: &Client,
    table: &str,
    column: &str,
    use_final: bool,
) -> Result<Option<(u64, u64)>> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct HashStatsRow {
        distinct_values: u64,
        total_rows: u64,
    }

    let final_clause = final_clause(use_final);
    let query = format!(
        "SELECT uniqExact({col}) AS distinct_values, count() AS total_rows \
         FROM {table}{final_clause}",
        col = column,
        table = table,
        final_clause = final_clause
    );

    let row: HashStatsRow = client.query(&query).fetch_one().await?;

    if row.total_rows == 0 {
        Ok(None)
    } else {
        Ok(Some((row.total_rows, row.distinct_values)))
    }
}

async fn optimize_table(_client: &Client, table: &str) -> Result<()> {
    info!(
        "Skipping `OPTIMIZE TABLE {} FINAL` because read-only mode is enabled.",
        table
    );
    Ok(())
}

async fn optimize_blocks_table(client: &Client, args: &Args) -> Result<()> {
    println!(
        "Issuing `OPTIMIZE TABLE ... FINAL` for blocks table `{}`.",
        args.blocks_table
    );
    let query = format!("OPTIMIZE TABLE {} FINAL", args.blocks_table);
    info!("Issuing `{}` against ClickHouse.", query);
    client.query(&query).execute().await.with_context(|| {
        format!(
            "Failed to optimize blocks table `{}` via `OPTIMIZE TABLE ... FINAL`",
            args.blocks_table
        )
    })?;
    println!(
        "Submitted `OPTIMIZE TABLE {} FINAL`; confirm completion on ClickHouse.",
        args.blocks_table
    );
    Ok(())
}

async fn optimize_transactions_table(client: &Client, args: &Args) -> Result<()> {
    println!(
        "Issuing `OPTIMIZE TABLE ... FINAL` for transactions table `{}`.",
        args.transactions_table
    );
    let query = format!("OPTIMIZE TABLE {} FINAL", args.transactions_table);
    info!("Issuing `{}` against ClickHouse.", query);
    client.query(&query).execute().await.with_context(|| {
        format!(
            "Failed to optimize transactions table `{}` via `OPTIMIZE TABLE ... FINAL`",
            args.transactions_table
        )
    })?;
    println!(
        "Submitted `OPTIMIZE TABLE {} FINAL`; confirm completion on ClickHouse.",
        args.transactions_table
    );
    Ok(())
}

async fn run_targeted_deletes(
    _client: &Client,
    table: &str,
    _column: &str,
) -> Result<Vec<(u64, u64)>> {
    println!(
        "Skipping DELETE queueing for table `{}` because read-only mode is enabled.",
        table
    );
    Ok(Vec::new())
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

async fn report_sync_status(provider: &RootProvider) -> Result<()> {
    println!("Checking Ethereum node sync status...");
    let status = provider
        .syncing()
        .await
        .context("Failed to query Ethereum node for sync status")?;

    match status {
        SyncStatus::None => {
            println!("Ethereum node is fully synchronized.");
        }
        SyncStatus::Info(info) => {
            let info = info.as_ref();
            println!("Ethereum node is still syncing:");
            println!("  Starting block: {}", info.starting_block);
            println!("  Current block: {}", info.current_block);
            println!("  Highest block: {}", info.highest_block);

            match (
                info.warp_chunks_processed.as_ref(),
                info.warp_chunks_amount.as_ref(),
            ) {
                (Some(processed), Some(total)) => {
                    println!("  Warp sync chunks processed: {}/{}", processed, total);
                }
                (Some(processed), None) => {
                    println!("  Warp sync chunks processed: {}", processed);
                }
                (None, Some(total)) => {
                    println!("  Warp sync total chunks: {}", total);
                }
                (None, None) => {}
            }

            if let Some(stages) = info.stages.as_ref() {
                if !stages.is_empty() {
                    println!("  Stage progress (showing up to 5 entries):");
                    for stage in stages.iter().take(5) {
                        println!("    {}: {}", stage.name, stage.block);
                    }
                    if stages.len() > 5 {
                        println!("    ...{} more stage(s)", stages.len() - 5);
                    }
                }
            }
        }
    }

    Ok(())
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
    println!("Backfill missing blocks from the Ethereum node? (y/N)");
    print!("> ");
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("Failed to read confirmation for filling missing blocks")?;

    let normalized = input.trim().to_lowercase();
    let decision = matches!(normalized.as_str(), "y" | "yes" | "1" | "true");
    Ok(decision)
}

fn prompt_repair_mismatches() -> Result<bool> {
    println!("Repair transaction mismatches using Ethereum node data? (y/N)");
    print!("> ");
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("Failed to read confirmation for repairing mismatched transactions")?;

    let normalized = input.trim().to_lowercase();
    let decision = matches!(normalized.as_str(), "y" | "yes" | "1" | "true");
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
            .ok_or_else(|| anyhow!("Failed to compute size for block range {}-{}", start, end))?;
        acc.checked_add(span)
            .ok_or_else(|| anyhow!("Total number of blocks to repair overflowed"))
    })?;

    println!(
        "Replaying {total_blocks} block(s) to resolve {mismatch_count} transaction mismatch(es).",
        total_blocks = total_blocks,
        mismatch_count = mismatches.len()
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

    let block_insert_columns = select_block_insert_columns(block_columns);
    if block_insert_columns.is_empty() {
        bail!(
            "No compatible columns found in blocks table `{}` for backfilling.",
            args.blocks_table
        );
    }

    let tx_insert_columns = select_transaction_insert_columns(tx_columns);
    if tx_columns.is_empty() {
        info!("Transaction table schema is empty; skipping transaction backfill.");
    } else if tx_insert_columns.is_empty() {
        println!(
            "Skipping transaction backfill for table `{}` because no compatible columns were found.",
            args.transactions_table
        );
        info!("Skipping INSERT for transactions table because no compatible columns exist.");
    }

    println!(
        "Backfilling {} missing range(s) from the Ethereum node into ClickHouse.",
        missing_ranges.len()
    );

    let mut total_blocks_inserted = 0usize;
    let mut total_transactions_inserted = 0usize;

    for &(start, end) in missing_ranges {
        println!("Backfilling block range {}-{}...", start, end);
        let mut block_rows: Vec<Vec<String>> = Vec::new();
        let mut tx_rows: Vec<Vec<String>> = Vec::new();
        let mut range_blocks_inserted = 0usize;
        let mut range_tx_inserted = 0usize;

        for number in start..=end {
            let block = load_block(provider, number).await?;

            let block_row = render_block_row(&block, &block_insert_columns)?;
            block_rows.push(block_row);

            if !tx_insert_columns.is_empty() {
                let mut rendered_txs = render_transaction_rows(&block, &tx_insert_columns)?;
                tx_rows.append(&mut rendered_txs);
            }

            if block_rows.len() >= INSERT_BATCH_SIZE {
                let pending = block_rows.len();
                insert_rows(
                    client,
                    &args.blocks_table,
                    &block_insert_columns,
                    &block_rows,
                )
                .await
                .with_context(|| {
                    format!(
                        "Failed to insert blocks into ClickHouse (range {}-{})",
                        start, end
                    )
                })?;
                block_rows.clear();
                range_blocks_inserted += pending;
                total_blocks_inserted += pending;
            }

            if tx_rows.len() >= INSERT_BATCH_SIZE {
                if !tx_insert_columns.is_empty() {
                    let pending = tx_rows.len();
                    insert_rows(
                        client,
                        &args.transactions_table,
                        &tx_insert_columns,
                        &tx_rows,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to insert transactions into ClickHouse (range {}-{})",
                            start, end
                        )
                    })?;
                    range_tx_inserted += pending;
                    total_transactions_inserted += pending;
                }
                tx_rows.clear();
            }
        }

        if !block_rows.is_empty() {
            let pending = block_rows.len();
            insert_rows(
                client,
                &args.blocks_table,
                &block_insert_columns,
                &block_rows,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to insert blocks into ClickHouse (range {}-{})",
                    start, end
                )
            })?;
            range_blocks_inserted += pending;
            total_blocks_inserted += pending;
        }

        if !tx_rows.is_empty() && !tx_insert_columns.is_empty() {
            let pending = tx_rows.len();
            insert_rows(
                client,
                &args.transactions_table,
                &tx_insert_columns,
                &tx_rows,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to insert transactions into ClickHouse (range {}-{})",
                    start, end
                )
            })?;
            range_tx_inserted += pending;
            total_transactions_inserted += pending;
        }

        println!(
            "  Completed backfill: {} block(s), {} transaction(s)",
            range_blocks_inserted, range_tx_inserted
        );
    }

    println!(
        "Finished backfilling missing data (total: {} block(s), {} transaction(s)).",
        total_blocks_inserted, total_transactions_inserted
    );
    Ok(())
}

fn select_block_insert_columns(columns: &[ColumnInfo]) -> Vec<&ColumnInfo> {
    columns
        .iter()
        .filter(|column| is_supported_block_column(&column.name))
        .collect()
}

fn select_transaction_insert_columns(columns: &[ColumnInfo]) -> Vec<&ColumnInfo> {
    columns
        .iter()
        .filter(|column| is_supported_transaction_column(&column.name))
        .collect()
}

async fn load_block(provider: &impl Provider, number: u64) -> Result<RpcBlock> {
    provider
        .get_block_by_number(BlockNumberOrTag::Number(number))
        .full()
        .await
        .with_context(|| format!("Failed to load block {number} from the Ethereum node"))?
        .ok_or_else(|| anyhow!("Ethereum node returned no data for block {number}"))
}

fn render_block_row(block: &RpcBlock, columns: &[&ColumnInfo]) -> Result<Vec<String>> {
    let mut values = Vec::with_capacity(columns.len());
    for column in columns {
        values.push(render_block_value(column, block)?);
    }
    Ok(values)
}

fn render_block_value(column: &ColumnInfo, block: &RpcBlock) -> Result<String> {
    let normalized = column.name.to_ascii_lowercase();
    let header = &block.header;

    let value = match normalized.as_str() {
        "number" => header.number().to_string(),
        "hash" => sql_string_literal(&format!("{:#x}", header.hash)),
        "parent_hash" => sql_string_literal(&format!("{:#x}", header.parent_hash())),
        "ommers_hash" => sql_string_literal(&format!("{:#x}", header.ommers_hash())),
        "timestamp" => header.timestamp().to_string(),
        "miner" => sql_string_literal(&format!("{:#x}", header.beneficiary())),
        "gas_limit" => header.gas_limit().to_string(),
        "gas_used" => header.gas_used().to_string(),
        "base_fee_per_gas" => match header.base_fee_per_gas {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        },
        "state_root" => sql_string_literal(&format!("{:#x}", header.state_root())),
        "transactions_root" => sql_string_literal(&format!("{:#x}", header.transactions_root())),
        "tx_count" => block.transactions.len().to_string(),
        "receipts_root" => sql_string_literal(&format!("{:#x}", header.receipts_root())),
        "logs_bloom" => sql_string_literal(&format!("{:#x}", header.logs_bloom())),
        "difficulty" => sql_string_literal(&header.difficulty().to_string()),
        "total_difficulty" => match header.total_difficulty {
            Some(value) => sql_string_literal(&value.to_string()),
            None => "NULL".to_string(),
        },
        "size_bytes" => match header.size {
            Some(size) => size.to_string(),
            None => "NULL".to_string(),
        },
        "extra_data" => sql_string_literal(&format!("{:#x}", header.extra_data)),
        "mix_hash" => header
            .mix_hash()
            .map(|value| sql_string_literal(&format!("{:#x}", value)))
            .unwrap_or_else(|| "NULL".to_string()),
        "nonce" => header
            .nonce()
            .map(|value| sql_string_literal(&format!("{:#x}", value)))
            .unwrap_or_else(|| "NULL".to_string()),
        "withdrawals_root" => match header.withdrawals_root {
            Some(root) => sql_string_literal(&format!("{:#x}", root)),
            None => "NULL".to_string(),
        },
        "blob_gas_used" => match header.blob_gas_used {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        },
        "excess_blob_gas" => match header.excess_blob_gas {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        },
        "parent_beacon_block_root" => match header.parent_beacon_block_root {
            Some(value) => sql_string_literal(&format!("{:#x}", value)),
            None => "NULL".to_string(),
        },
        "requests_hash" => match header.requests_hash {
            Some(value) => sql_string_literal(&format!("{:#x}", value)),
            None => "NULL".to_string(),
        },
        "uncles" => {
            let items: Vec<String> = block
                .uncles
                .iter()
                .map(|hash| sql_string_literal(&format!("{:#x}", hash)))
                .collect();
            sql_array_literal(&items)
        }
        "withdrawals" => match &block.withdrawals {
            Some(withdrawals) => {
                let json_value = json!(withdrawals);
                sql_string_literal(&json_value.to_string())
            }
            None => "NULL".to_string(),
        },
        "version" => "now()".to_string(),
        other => {
            bail!("Block column `{}` is not supported for backfilling", other);
        }
    };

    Ok(value)
}

fn render_transaction_rows(block: &RpcBlock, columns: &[&ColumnInfo]) -> Result<Vec<Vec<String>>> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let Some(transactions) = block.transactions.as_transactions() else {
        bail!(
            "Block {} returned only transaction hashes; enable full() in the RPC call.",
            block.header.number()
        );
    };

    let block_hash = block.header.hash;
    let block_number = block.header.number();
    let base_fee = block.header.base_fee_per_gas;

    let mut rows = Vec::with_capacity(transactions.len());
    for (idx, tx) in transactions.iter().enumerate() {
        let mut values = Vec::with_capacity(columns.len());
        for column in columns {
            values.push(render_transaction_value(
                column,
                tx,
                block_hash,
                block_number,
                idx as u64,
                base_fee,
            )?);
        }
        rows.push(values);
    }

    Ok(rows)
}

fn render_transaction_value(
    column: &ColumnInfo,
    tx: &RpcTransaction,
    block_hash: B256,
    block_number: u64,
    tx_index: u64,
    base_fee: Option<u64>,
) -> Result<String> {
    let normalized = column.name.to_ascii_lowercase();
    let signature = extract_signature(tx);

    let value = match normalized.as_str() {
        "hash" => sql_string_literal(&format!("{:#x}", tx.tx_hash())),
        "block_hash" => sql_string_literal(&format!("{:#x}", block_hash)),
        "block_number" => block_number.to_string(),
        "transaction_index" => tx_index.to_string(),
        "from_address" => sql_string_literal(&format!("{:#x}", tx.from())),
        "to_address" => match ConsensusTransactionTrait::kind(tx) {
            TxKind::Call(address) => sql_string_literal(&format!("{:#x}", address)),
            TxKind::Create => "NULL".to_string(),
        },
        "value" => sql_string_literal(&ConsensusTransactionTrait::value(tx).to_string()),
        "nonce" => ConsensusTransactionTrait::nonce(tx).to_string(),
        "gas_limit" => ConsensusTransactionTrait::gas_limit(tx).to_string(),
        "gas_price" => match ConsensusTransactionTrait::gas_price(tx) {
            Some(value) => sql_string_literal(&value.to_string()),
            None => "NULL".to_string(),
        },
        "max_fee_per_gas" => {
            sql_string_literal(&ConsensusTransactionTrait::max_fee_per_gas(tx).to_string())
        }
        "max_priority_fee_per_gas" => match ConsensusTransactionTrait::max_priority_fee_per_gas(tx)
        {
            Some(value) => sql_string_literal(&value.to_string()),
            None => "NULL".to_string(),
        },
        "max_fee_per_blob_gas" => match ConsensusTransactionTrait::max_fee_per_blob_gas(tx) {
            Some(value) => sql_string_literal(&value.to_string()),
            None => "NULL".to_string(),
        },
        "effective_gas_price" => sql_string_literal(
            &ConsensusTransactionTrait::effective_gas_price(tx, base_fee).to_string(),
        ),
        "transaction_type" => Typed2718::ty(tx).to_string(),
        "chain_id" => match ConsensusTransactionTrait::chain_id(tx) {
            Some(chain_id) => chain_id.to_string(),
            None => "NULL".to_string(),
        },
        "access_list" => match ConsensusTransactionTrait::access_list(tx) {
            Some(list) => {
                let json_value = json!(list);
                sql_string_literal(&json_value.to_string())
            }
            None => "NULL".to_string(),
        },
        "blob_versioned_hashes" => match ConsensusTransactionTrait::blob_versioned_hashes(tx) {
            Some(hashes) => {
                let items: Vec<String> = hashes
                    .iter()
                    .map(|hash| sql_string_literal(&format!("{:#x}", hash)))
                    .collect();
                sql_array_literal(&items)
            }
            None => "[]".to_string(),
        },
        "authorization_list" => match ConsensusTransactionTrait::authorization_list(tx) {
            Some(list) => {
                let json_value = json!(list);
                sql_string_literal(&json_value.to_string())
            }
            None => "NULL".to_string(),
        },
        "input" => sql_string_literal(&format!("{:#x}", ConsensusTransactionTrait::input(tx))),
        "y_parity" => match signature {
            Some(sig) => (sig.v() as u8).to_string(),
            None => "NULL".to_string(),
        },
        "v" => match signature {
            Some(sig) => {
                let tx_type = Typed2718::ty(tx);
                let parity = if sig.v() { 1u64 } else { 0u64 };
                let legacy_v = if tx_type == 0 {
                    match ConsensusTransactionTrait::chain_id(tx) {
                        Some(chain_id) => chain_id
                            .saturating_mul(2)
                            .saturating_add(35)
                            .saturating_add(parity),
                        None => 27 + parity,
                    }
                } else {
                    27 + parity
                };
                legacy_v.to_string()
            }
            None => "NULL".to_string(),
        },
        "r" => match signature {
            Some(sig) => sql_string_literal(&format!("{:#x}", sig.r())),
            None => "NULL".to_string(),
        },
        "s" => match signature {
            Some(sig) => sql_string_literal(&format!("{:#x}", sig.s())),
            None => "NULL".to_string(),
        },
        "version" => "now()".to_string(),
        other => {
            bail!(
                "Transaction column `{}` is not supported for backfilling",
                other
            );
        }
    };

    Ok(value)
}

async fn insert_rows(
    client: &Client,
    table: &str,
    columns: &[&ColumnInfo],
    rows: &[Vec<String>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let column_list = columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");

    let values = rows
        .iter()
        .map(|row| format!("({})", row.join(", ")))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        sql_identifier(table),
        column_list,
        values
    );

    client
        .query(&sql)
        .execute()
        .await
        .with_context(|| format!("Failed to execute query `{}`", sql))
}

fn sql_identifier(identifier: &str) -> String {
    identifier
        .split('.')
        .map(|part| {
            let mut escaped = String::with_capacity(part.len() + 2);
            escaped.push('`');
            for ch in part.chars() {
                if ch == '`' || ch == '\\' {
                    escaped.push('\\');
                }
                escaped.push(ch);
            }
            escaped.push('`');
            escaped
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn sql_string_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('\'');
    for ch in value.chars() {
        match ch {
            '\\' | '\'' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped.push('\'');
    escaped
}

fn sql_array_literal(items: &[String]) -> String {
    if items.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", items.join(", "))
    }
}

fn is_supported_block_column(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "number"
            | "hash"
            | "parent_hash"
            | "ommers_hash"
            | "timestamp"
            | "miner"
            | "gas_limit"
            | "gas_used"
            | "base_fee_per_gas"
            | "state_root"
            | "transactions_root"
            | "tx_count"
            | "receipts_root"
            | "logs_bloom"
            | "difficulty"
            | "total_difficulty"
            | "size_bytes"
            | "extra_data"
            | "mix_hash"
            | "nonce"
            | "withdrawals_root"
            | "blob_gas_used"
            | "excess_blob_gas"
            | "parent_beacon_block_root"
            | "requests_hash"
            | "uncles"
            | "withdrawals"
            | "version"
    )
}

fn is_supported_transaction_column(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "block_number"
            | "block_hash"
            | "transaction_index"
            | "hash"
            | "from_address"
            | "to_address"
            | "value"
            | "nonce"
            | "gas_limit"
            | "gas_price"
            | "max_fee_per_gas"
            | "max_priority_fee_per_gas"
            | "max_fee_per_blob_gas"
            | "effective_gas_price"
            | "transaction_type"
            | "chain_id"
            | "access_list"
            | "blob_versioned_hashes"
            | "authorization_list"
            | "input"
            | "y_parity"
            | "v"
            | "r"
            | "s"
            | "version"
    )
}

fn extract_signature(tx: &RpcTransaction) -> Option<&Signature> {
    match tx.inner.as_ref() {
        TxEnvelope::Legacy(signed) => Some(signed.signature()),
        TxEnvelope::Eip2930(signed) => Some(signed.signature()),
        TxEnvelope::Eip1559(signed) => Some(signed.signature()),
        TxEnvelope::Eip4844(signed) => Some(signed.signature()),
        TxEnvelope::Eip7702(signed) => Some(signed.signature()),
    }
}

fn print_columns(columns: &[ColumnInfo], table: &str, label: &str) {
    if columns.is_empty() {
        println!(
            "Table `{}` ({}) has no columns or does not exist.",
            table, label
        );
    } else {
        println!("Table `{}` ({}) columns:", table, label);
        for column in columns {
            println!("  {} {}", column.name, column.column_type);
        }
    }
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
    println!("Data integrity:");
    println!("  1) Block gap detection");
    println!("  2) Transaction table gap detection");
    println!("  3) Receipts table gap detection");
    println!("  4) Transaction count mismatch detection");
    println!("Duplicates:");
    println!("  5) Duplicate block detection");
    println!("  6) Duplicate transaction hash detection");
    println!("  7) Duplicate receipt hash detection");
    println!("Operations:");
    println!("  8) Unfinished mutation count");
    println!("  9) Show table columns");
    println!("  10) Optimize both tables (blocks + transactions)");
    println!("  11) Optimize blocks table only");
    println!("  12) Optimize transactions table only");
    println!("Node status:");
    println!("  13) Fetch Ethereum node sync status");
    println!("Enter numbers separated by commas (e.g. `1,5`) or press Enter for defaults:");
    print!("> ");
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    let read = io::stdin()
        .read_line(&mut input)
        .context("Failed to read selection from stdin")?;

    let mut selection = SelectedChecks {
        block_gap: false,
        tx_gap: false,
        receipts_gap: false,
        tx_mismatch: false,
        duplicates: false,
        tx_duplicates: false,
        receipts_duplicates: false,
        mutations: false,
        optimize_blocks: false,
        optimize_transactions: false,
        sync_status: false,
        show_schema: false,
    };

    if read == 0 || input.trim().is_empty() {
        selection.block_gap = true;
        selection.tx_gap = true;
        selection.tx_mismatch = true;
        selection.duplicates = true;
        selection.tx_duplicates = true;
        selection.mutations = true;
        selection.optimize_blocks = false;
        selection.optimize_transactions = false;
        selection.sync_status = true;
        selection.show_schema = true;
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
            "2"
            | "tx_gap"
            | "transaction_gap"
            | "transactions_gap"
            | "transactions_block_gap" => {
                selection.tx_gap = true;
            }
            "3" | "receipts_gap" | "receipt_gap" | "receipts_block_gap" => {
                selection.receipts_gap = true;
            }
            "4" | "tx" | "transactions" | "transaction" | "tx_mismatch" => {
                selection.tx_mismatch = true;
            }
            "5" | "dup" | "duplicate" | "duplicates" => {
                selection.duplicates = true;
            }
            "6"
            | "tx_duplicates"
            | "duplicate_transactions"
            | "duplicate-tx"
            | "duplicate_hashes" => {
                selection.tx_duplicates = true;
            }
            "7"
            | "receipts_duplicates"
            | "receipt_duplicates"
            | "duplicate_receipts"
            | "duplicate-receipts" => {
                selection.receipts_duplicates = true;
            }
            "8" | "mutation" | "mutations" | "unfinished" | "unfinished_mutations" => {
                selection.mutations = true;
            }
            "9" | "schema" | "columns" | "show_schema" | "show-columns" => {
                selection.show_schema = true;
            }
            "10" | "optimize" | "optimize_all" | "optimize-both" => {
                selection.optimize_blocks = true;
                selection.optimize_transactions = true;
            }
            "11" | "optimize_blocks" | "optimize-blocks" => {
                selection.optimize_blocks = true;
            }
            "12" | "optimize_tx" | "optimize_transactions" | "optimize-transactions" => {
                selection.optimize_transactions = true;
            }
            "13" | "sync" | "sync_status" | "syncing" | "eth_sync" => {
                selection.sync_status = true;
            }
            "all" | "a" => {
                selection.block_gap = true;
                selection.tx_gap = true;
                selection.receipts_gap = true;
                selection.tx_mismatch = true;
                selection.duplicates = true;
                selection.tx_duplicates = true;
                selection.receipts_duplicates = true;
                selection.mutations = true;
                selection.optimize_blocks = true;
                selection.optimize_transactions = true;
                selection.sync_status = true;
                selection.show_schema = true;
            }
            other => {
                bail!("Unknown selection: `{}`", other);
            }
        }
    }

    if !selection.block_gap
        && !selection.tx_gap
        && !selection.receipts_gap
        && !selection.tx_mismatch
        && !selection.duplicates
        && !selection.tx_duplicates
        && !selection.receipts_duplicates
        && !selection.mutations
        && !selection.optimize_blocks
        && !selection.optimize_transactions
        && !selection.sync_status
        && !selection.show_schema
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
