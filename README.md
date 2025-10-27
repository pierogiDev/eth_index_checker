# Ethereum Index Coverage Checker

ClickHouse に保存された Ethereum のブロック／トランザクションデータが欠けていないかを検査するための簡単な CLI ツールです。以下の 2 点を確認します。

- ブロック番号が最小値から最大値まで連番になっているか（欠番の検出）
- `transactions` テーブルに登録されている件数と、Ethereum ノードから取得した実際のトランザクション件数が一致しているか
- 実行開始時に対象テーブルのカラム一覧を確認用に表示

トランザクション件数の照合には [alloy](https://crates.io/crates/alloy) を利用し、ノードの `eth_getBlockTransactionCountByNumber` を呼び出して確認します。

## 使い方

1. Rust/Cargo を用意します（`cargo run` を利用）。
2. `.env.example` を参考に `.env` を作成し、ClickHouse の接続情報（アドレス／ポート／ユーザー名／パスワード／データベース名）と Ethereum ノードの RPC エンドポイントを記入します。ビルド時にこれらの値がバイナリへ埋め込まれます。
3. 追加で上書きしたい項目があれば CLI オプションや環境変数を使います（`.env` の値が既定値になります）。

```bash
cp .env.example .env
# .env を編集

cargo run --release
```

### 主なオプション

| オプション | 環境変数 | 既定値 | 説明 |
|-----------|----------|--------|------|
| `--address` | `CLICKHOUSE_ADDRESS` | `.env` の値 / `http://localhost` | ClickHouse ベースアドレス（スキームを含める） |
| `--port` | `CLICKHOUSE_PORT` | `.env` の値 / `8123` | ClickHouse HTTP ポート |
| `--database` | `CLICKHOUSE_DATABASE` | `.env` の値 / `eth_indexer` | 対象データベース（スキーマ名） |
| `--user` | `CLICKHOUSE_USER` | `.env` の値 / `default` | ユーザー名 |
| `--password` | `CLICKHOUSE_PASSWORD` | `.env` の値 / 空文字 | パスワード |
| `--blocks-table` | `BLOCKS_TABLE` | `blocks` | ブロックテーブル名 |
| `--blocks-number-column` | `BLOCKS_NUMBER_COLUMN` | `number` | ブロック番号カラム名 |
| `--transactions-table` | `TRANSACTIONS_TABLE` | `transactions` | トランザクションテーブル名 |
| `--transactions-block-column` | `TRANSACTIONS_BLOCK_COLUMN` | `block_number` | トランザクション側のブロック番号カラム名 |
| `--eth-node-url` | `ETH_NODE_URL` | `.env` の値 / `http://localhost:8545` | Ethereum ノード RPC エンドポイント（HTTP/HTTPS） |

## 実行結果

- ブロック範囲（最小値／最大値／件数）
- 欠番の総数と範囲（存在する場合）
- ClickHouse 側の件数と Ethereum ノードが返す件数が一致しないブロック一覧（存在する場合）

ClickHouse クエリは集計のみを行い、データの更新・削除は一切行いません。

## コントリビューション

開発やレビューに参加する場合は、`AGENTS.md` を参照してガイドラインに従ってください。特に、プロジェクト内のコミュニケーションは日本語で行い、共有するログや出力にも必要に応じて日本語で補足説明を追加してください。
