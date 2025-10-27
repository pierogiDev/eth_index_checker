# Repository Guidelines

## Project Structure & Module Organization
The CLI entry point lives in `src/main.rs` for argument parsing, ClickHouse queries, and Ethereum RPC checks. Build-time configuration in `build.rs` reads `.env` and emits compile-time defaults, so secrets must stay out of Git. Use `.env.example` as the template for a local `.env`, and treat `target/` as disposable build output. When expanding features, keep the CLI layout and add integration cases under `tests/` as they appear.

## Build, Test, and Development Commands
- `cargo build` – Compile in debug mode for quick iteration.
- `cargo run --release -- --address http://localhost` – Execute the checker with optimized settings; forward runtime flags after `--`.
- `cargo test` – Run the test suite (add tests before relying on this output).
- `cargo fmt` – Auto-format the codebase using `rustfmt`.
- `cargo clippy -- -D warnings` – Lint with Clippy and fail on warnings; run before opening a PR.

## Coding Style & Naming Conventions
Follow Rust 2021 idioms with 4-space indentation and rely on `cargo fmt` for formatting. Prefer snake_case for functions and locals, UpperCamelCase for types, and SCREAMING_SNAKE for env-backed constants. Factor ClickHouse queries into helpers and derive traits like `Debug` or `Clone` instead of hand-written implementations.

## Testing Guidelines
Unit tests belong in-module behind `#[cfg(test)]`; integration tests live in `tests/` with names like `blocks_gap.rs`. Cover missing blocks, mismatched transaction counts, and duplicate detection, mocking ClickHouse when the real service is unavailable. Target coverage of every user-facing flag before cutting a release.

## Commit & Pull Request Guidelines
The repository has no history yet; start with present-tense summaries such as `Add duplicate detection report`. Group related changes into focused commits and add body notes when behavior shifts. Pull requests should explain the motivation, list verification steps, link issues, and confirm that CI is green with secrets redacted.

## Communication Guidelines
プロジェクト内のやりとりは常に日本語で行ってください。英語のログや出力を共有する場合でも、要点を簡潔な日本語で補足することを推奨します。

## Configuration & Security Notes
Confirm `.env` values before compiling; `build.rs` embeds them during `cargo build`. Rotate production credentials regularly, keep `.env` out of commits, and redact hostnames or keys in shared logs. Use env overrides (`CLICKHOUSE_ADDRESS`, `ETH_NODE_URL`) for temporary adjustments.
