# Webhook transaction store (rebuild)

## 概要
- 親テーブル: `transactions`（1取引1行）
- 子テーブル: `webhook_events`（Webhook受信履歴）

## 仕様
- 親の `current_status` は「最後に受信した子データの status」を採用。
- 親に採用する値は `amount`, `email`, `payer_name`。
- 子には `payload_json` をそのまま保存。

## 実行
```bash
php -S 0.0.0.0:8000
```

Webhook endpoint:
- `POST /webhook.php`

## テスト例
```bash
curl -X POST http://127.0.0.1:8000/webhook.php \
  -H 'Content-Type: application/json' \
  -d '{"id":"11f13fa5-fe1a-1ee6-a6ae-0fab86de144d","status":"pending","amount":500000,"email":"a@example.com","name":"Taro"}'
```