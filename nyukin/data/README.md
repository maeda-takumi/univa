# CSV配置ルール

このアプリは、`data` フォルダ内のCSVをページ表示時に毎回読み込みます。

## 必須ファイル

| ファイル名 | 内容 |
|---|---|
| `customer_payments.csv` | 自社DBからエクスポートした顧客・入金データ |
| `univapay_transactions.csv` | UnivaPayから取得した取引履歴データ |

## 更新方法

CSVを更新する場合は、同じファイル名で上書きしてください。

- 自社DBのCSVを更新する場合: `data/customer_payments.csv`
- UnivaPayのCSVを更新する場合: `data/univapay_transactions.csv`

PHP版のページはHTMLと違い、CSVをページ表示時に読み込むため、CSV上書き後にブラウザを再読み込みすれば最新データが反映されます。

## 注意

ファイル名を変更すると読み込めません。必ず上記の固定ファイル名で配置してください。
