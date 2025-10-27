# Ethereum Index Coverage Checker

ClickHouse に保存された Ethereum のブロック／トランザクションデータが欠けていないかを検査する CLI ツールです。ブロック欠番やトランザクション件数の不整合を検出し、運用中のインデックスの品質を素早く把握できます。

## 特徴

- ブロック番号の連続性をチェックして欠番を報告
- ClickHouse に格納されたトランザクション件数と Ethereum ノードの実数を照合
- 実行開始時に対象テーブルのカラム一覧を表示し、設定ミスを早期発見

照合作業では [alloy](https://crates.io/crates/alloy) を用いて `eth_getBlockTransactionCountByNumber` を呼び出します。

## 必要条件

- Rust 1.74 以降（`rustup` でのインストールを推奨）
- ClickHouse への HTTP 接続情報
- Ethereum ノードの RPC エンドポイント（HTTP/HTTPS）

## セットアップ

1. Rust/Cargo をインストールします。
2. `.env.example` を複製して `.env` を作り、ClickHouse と Ethereum ノードの接続情報を記入します。ビルド時に `build.rs` が `.env` を読み込み、既定値としてバイナリに埋め込みます。

   ```bash
   cp .env.example .env
   # .env を編集
   ```

3. `.env` の値を一時的に変えたい場合は環境変数または CLI オプションで上書きしてください。

## 使い方

`.env` を準備したら、以下のコマンドで検査を実行します。`--` 以降がツールへの引数です。

```bash
cargo run --release -- \
  --address http://localhost:8123 \
  --database eth_indexer \
  --eth-node-url http://localhost:8545
```

### CLI オプション

| オプション | 環境変数 | 既定値 | 説明 |
|-----------|----------|--------|------|
| `--address` | `CLICKHOUSE_ADDRESS` | `.env` の値 / `http://localhost` | ClickHouse ベースアドレス（スキーム必須） |
| `--port` | `CLICKHOUSE_PORT` | `.env` の値 / `8123` | ClickHouse HTTP ポート |
| `--database` | `CLICKHOUSE_DATABASE` | `.env` の値 / `eth_indexer` | 対象データベース名 |
| `--user` | `CLICKHOUSE_USER` | `.env` の値 / `default` | 接続ユーザー名 |
| `--password` | `CLICKHOUSE_PASSWORD` | `.env` の値 / 空文字 | 接続パスワード |
| `--blocks-table` | `BLOCKS_TABLE` | `blocks` | ブロックテーブル名 |
| `--blocks-number-column` | `BLOCKS_NUMBER_COLUMN` | `number` | ブロック番号カラム名 |
| `--transactions-table` | `TRANSACTIONS_TABLE` | `transactions` | トランザクションテーブル名 |
| `--transactions-block-column` | `TRANSACTIONS_BLOCK_COLUMN` | `block_number` | トランザクション側のブロック番号カラム名 |
| `--eth-node-url` | `ETH_NODE_URL` | `.env` の値 / `http://localhost:8545` | Ethereum ノード RPC エンドポイント |

## 実行結果

- ブロック範囲（最小値／最大値／件数）
- 欠番の総数と範囲（存在する場合）
- ClickHouse と Ethereum ノードで件数が不一致なブロック一覧（存在する場合）

ClickHouse へのアクセスは集計系クエリのみで、既存データを更新・削除することはありません。

## 開発の進め方

- `cargo build`：デバッグビルドで動作確認
- `cargo fmt`：Rustfmt による自動整形
- `cargo clippy -- -D warnings`：Lint を実行（警告をエラーとして扱う）
- `cargo test`：ユニットテスト／統合テストを実行

テストコードはモジュール内で `#[cfg(test)]` を付けるか、`tests/` 以下に統合テストを追加してください。ユーザー向けフラグを追加した場合は対応するテストケースも用意します。

## コントリビューション

開発やレビューに参加する際は `AGENTS.md` を参照し、コミットメッセージや PR 説明、ログ共有のガイドラインに従ってください。特に、プロジェクト内のやりとりは日本語で行い、英語のログを共有する場合も要点を日本語で補足してください。
