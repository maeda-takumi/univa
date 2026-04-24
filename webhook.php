<?php
// Univapay Webhook Receiver
// Save this file as webhook.php on your server.
// It receives webhook POSTs and stores them in SQLite.

declare(strict_types=1);

// =========================
// Basic settings
// =========================
const DB_DIR = __DIR__ . '/data';
const DB_FILE = DB_DIR . '/univapay_webhook.sqlite';
const RAW_DB_FILE = DB_DIR . '/univapay_webhook_raw.sqlite';
const LOG_FILE = DB_DIR . '/univapay_webhook_error.log';

// Optional shared secret check.
// If you set a value here, the request must include the same value
// in the Authorization header.
const EXPECTED_AUTHORIZATION = '';

// =========================
// Helpers
// =========================
function send_json(int $statusCode, array $data): void
{
    http_response_code($statusCode);
    header('Content-Type: application/json; charset=UTF-8');
    echo json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

function get_request_headers_case_insensitive(): array
{
    $headers = [];

    foreach ($_SERVER as $key => $value) {
        if (strpos($key, 'HTTP_') === 0) {
            $name = str_replace('_', '-', strtolower(substr($key, 5)));
            $headers[$name] = $value;
        }
    }

    if (function_exists('getallheaders')) {
        foreach (getallheaders() as $name => $value) {
            $headers[strtolower($name)] = $value;
        }
    }

    return $headers;
}

function ensure_data_dir_exists(string $dir): void
{
    if (!is_dir($dir) && !mkdir($dir, 0777, true) && !is_dir($dir)) {
        throw new RuntimeException('dataフォルダの作成に失敗しました: ' . $dir);
    }
}

function get_pdo(): PDO
{
    ensure_data_dir_exists(DB_DIR);

    $pdo = new PDO('sqlite:' . DB_FILE);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

    $pdo->exec(
        'CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            request_method TEXT,
            remote_addr TEXT,
            user_agent TEXT,
            authorization_header TEXT,
            content_type TEXT,
            event_type TEXT,
            status TEXT,
            status_raw TEXT,
            transaction_id TEXT,
            charge_id TEXT,
            store_id TEXT,
            customer_id TEXT,
            amount INTEGER,
            currency TEXT,
            livemode INTEGER,
            raw_json TEXT NOT NULL
        )'
    );

    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_events_received_at ON webhook_events(received_at)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_events_event_type ON webhook_events(event_type)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_events_transaction_id ON webhook_events(transaction_id)');

    $columns = $pdo->query('PRAGMA table_info(webhook_events)')->fetchAll();
    $hasStatusRaw = false;
    foreach ($columns as $column) {
        if (($column['name'] ?? null) === 'status_raw') {
            $hasStatusRaw = true;
            break;
        }
    }
    if (!$hasStatusRaw) {
        $pdo->exec('ALTER TABLE webhook_events ADD COLUMN status_raw TEXT');
        $pdo->exec("UPDATE webhook_events SET status_raw = status WHERE status_raw IS NULL");
    }
    $hasSource = false;
    foreach ($columns as $column) {
        if (($column['name'] ?? null) === 'source') {
            $hasSource = true;
            break;
        }
    }
    if (!$hasSource) {
        $pdo->exec("ALTER TABLE webhook_events ADD COLUMN source TEXT NOT NULL DEFAULT 'WEBHOOK'");
    }

    return $pdo;
}

function get_raw_pdo(): PDO
{
    ensure_data_dir_exists(DB_DIR);

    $pdo = new PDO('sqlite:' . RAW_DB_FILE);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

    $pdo->exec(
        'CREATE TABLE IF NOT EXISTS webhook_raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            request_method TEXT,
            remote_addr TEXT,
            user_agent TEXT,
            authorization_header TEXT,
            content_type TEXT,
            event_type TEXT,
            status_raw TEXT,
            transaction_id TEXT,
            charge_id TEXT,
            store_id TEXT,
            customer_id TEXT,
            amount INTEGER,
            currency TEXT,
            livemode INTEGER,
            raw_json TEXT NOT NULL
        )'
    );

    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_raw_events_received_at ON webhook_raw_events(received_at)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_raw_events_event_type ON webhook_raw_events(event_type)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_raw_events_transaction_id ON webhook_raw_events(transaction_id)');
    return $pdo;
}

function first_non_empty(array $values): ?string
{
    foreach ($values as $value) {
        if ($value !== null && $value !== '') {
            return (string)$value;
        }
    }
    return null;
}

function first_non_empty_int(array $values): ?int
{
    foreach ($values as $value) {
        if ($value !== null && $value !== '' && is_numeric($value)) {
            return (int)$value;
        }
    }
    return null;
}

function get_nested(array $source, array $path)
{
    $current = $source;
    foreach ($path as $key) {
        if (!is_array($current) || !array_key_exists($key, $current)) {
            return null;
        }
        $current = $current[$key];
    }
    return $current;
}

function write_error_log(string $message): void
{
    ensure_data_dir_exists(DB_DIR);
    $line = '[' . date('Y-m-d H:i:s') . '] ' . $message . PHP_EOL;
    file_put_contents(LOG_FILE, $line, FILE_APPEND);
}

function translate_status_to_japanese(?string $statusRaw): ?string
{
    if ($statusRaw === null) {
        return null;
    }

    $normalized = strtolower(trim($statusRaw));
    if ($normalized === '') {
        return null;
    }

    if (
        str_contains($normalized, 'success') ||
        str_contains($normalized, 'succeeded') ||
        str_contains($normalized, 'completed') ||
        str_contains($normalized, 'paid') ||
        str_contains($normalized, 'captured') ||
        str_contains($normalized, 'approved')
    ) {
        return '成功';
    }

    if (
        str_contains($normalized, 'pending') ||
        str_contains($normalized, 'processing') ||
        str_contains($normalized, 'in_progress') ||
        str_contains($normalized, 'authorized') ||
        str_contains($normalized, 'awaiting')
    ) {
        return '処理中';
    }

    if (
        str_contains($normalized, 'refund') ||
        str_contains($normalized, 'chargeback') ||
        str_contains($normalized, 'reversed')
    ) {
        return '返金/取消';
    }

    if (
        str_contains($normalized, 'fail') ||
        str_contains($normalized, 'cancel') ||
        str_contains($normalized, 'error') ||
        str_contains($normalized, 'expired') ||
        str_contains($normalized, 'declined') ||
        str_contains($normalized, 'voided')
    ) {
        return '失敗';
    }

    return $statusRaw;
}
// =========================
// Main
// =========================
try {
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        send_json(405, [
            'ok' => false,
            'message' => 'POST only',
        ]);
    }

    $headers = get_request_headers_case_insensitive();
    $authorization = $headers['authorization'] ?? '';

    if (EXPECTED_AUTHORIZATION !== '' && $authorization !== EXPECTED_AUTHORIZATION) {
        send_json(401, [
            'ok' => false,
            'message' => 'Unauthorized',
        ]);
    }

    $rawBody = file_get_contents('php://input');
    if ($rawBody === false || trim($rawBody) === '') {
        send_json(400, [
            'ok' => false,
            'message' => 'Empty body',
        ]);
    }

    $payload = json_decode($rawBody, true);
    if (!is_array($payload)) {
        send_json(400, [
            'ok' => false,
            'message' => 'Invalid JSON',
        ]);
    }

    // Univapay payload shapes may vary by event.
    // We store the raw JSON no matter what, and also try to extract common fields.
    $eventType = first_non_empty([
        $payload['event'] ?? null,
        $payload['event_type'] ?? null,
        $payload['type'] ?? null,
        get_nested($payload, ['data', 'event'])
    ]);

    $statusRaw = first_non_empty([
        $payload['status'] ?? null,
        get_nested($payload, ['data', 'status']),
        get_nested($payload, ['data', 'three_ds', 'status']),
        get_nested($payload, ['data', 'data', 'three_ds', 'status']),
        get_nested($payload, ['transaction', 'status']),
        get_nested($payload, ['charge', 'status'])
    ]);
    $status = translate_status_to_japanese($statusRaw);

    $transactionId = first_non_empty([
        $payload['transaction_id'] ?? null,
        $payload['id'] ?? null,
        get_nested($payload, ['data', 'transaction_id']),
        get_nested($payload, ['data', 'transaction_token_id']),
        get_nested($payload, ['transaction', 'id'])
    ]);

    $chargeId = first_non_empty([
        $payload['charge_id'] ?? null,
        get_nested($payload, ['data', 'charge_id']),
        get_nested($payload, ['data', 'id']),
        get_nested($payload, ['charge', 'id'])
    ]);

    $storeId = first_non_empty([
        $payload['store_id'] ?? null,
        get_nested($payload, ['data', 'store_id']),
        get_nested($payload, ['store', 'id'])
    ]);

    $customerId = first_non_empty([
        $payload['customer_id'] ?? null,
        get_nested($payload, ['data', 'customer_id']),
        get_nested($payload, ['customer', 'id']),
        get_nested($payload, ['data', 'email']),
        get_nested($payload, ['data', 'metadata', 'univapay-name']),
        get_nested($payload, ['data', 'metadata', 'univapay-phone-number'])
    ]);

    $amount = first_non_empty_int([
        $payload['amount'] ?? null,
        get_nested($payload, ['data', 'amount']),
        get_nested($payload, ['data', 'charged_amount']),
        get_nested($payload, ['data', 'requested_amount']),
        get_nested($payload, ['transaction', 'amount']),
        get_nested($payload, ['charge', 'amount'])
    ]);

    $currency = first_non_empty([
        $payload['currency'] ?? null,
        get_nested($payload, ['data', 'currency']),
        get_nested($payload, ['data', 'charged_currency']),
        get_nested($payload, ['data', 'requested_currency']),
        get_nested($payload, ['transaction', 'currency']),
        get_nested($payload, ['charge', 'currency'])
    ]);

    $livemodeRaw = $payload['livemode']
        ?? get_nested($payload, ['data', 'livemode'])
        ?? null;
    $livemode = is_bool($livemodeRaw) ? ($livemodeRaw ? 1 : 0) : null;

    $receivedAt = date('Y-m-d H:i:s');
    $commonInsertParams = [
        ':received_at' => $receivedAt,
        ':request_method' => $_SERVER['REQUEST_METHOD'] ?? null,
        ':remote_addr' => $_SERVER['REMOTE_ADDR'] ?? null,
        ':user_agent' => $_SERVER['HTTP_USER_AGENT'] ?? null,
        ':authorization_header' => $authorization !== '' ? $authorization : null,
        ':content_type' => $_SERVER['CONTENT_TYPE'] ?? null,
        ':event_type' => $eventType,
        ':status_raw' => $statusRaw,
        ':transaction_id' => $transactionId,
        ':charge_id' => $chargeId,
        ':store_id' => $storeId,
        ':customer_id' => $customerId,
        ':amount' => $amount,
        ':currency' => $currency,
        ':livemode' => $livemode,
        ':raw_json' => $rawBody,
    ];

    $rawPdo = get_raw_pdo();
    $rawStmt = $rawPdo->prepare(
        'INSERT INTO webhook_raw_events (
            received_at,
            request_method,
            remote_addr,
            user_agent,
            authorization_header,
            content_type,
            event_type,
            status_raw,
            transaction_id,
            charge_id,
            store_id,
            customer_id,
            amount,
            currency,
            livemode,
            raw_json
        ) VALUES (
            :received_at,
            :request_method,
            :remote_addr,
            :user_agent,
            :authorization_header,
            :content_type,
            :event_type,
            :status_raw,
            :transaction_id,
            :charge_id,
            :store_id,
            :customer_id,
            :amount,
            :currency,
            :livemode,
            :raw_json
        )'
    );
    $rawStmt->execute($commonInsertParams);
    $pdo = get_pdo();

    $stmt = $pdo->prepare(
        'INSERT INTO webhook_events (
            received_at,
            request_method,
            remote_addr,
            user_agent,
            authorization_header,
            content_type,
            event_type,
            status,
            status_raw,
            transaction_id,
            charge_id,
            store_id,
            customer_id,
            amount,
            currency,
            livemode,
            source,
            raw_json
        ) VALUES (
            :received_at,
            :request_method,
            :remote_addr,
            :user_agent,
            :authorization_header,
            :content_type,
            :event_type,
            :status,
            :status_raw,
            :transaction_id,
            :charge_id,
            :store_id,
            :customer_id,
            :amount,
            :currency,
            :livemode,
            :source,
            :raw_json
        )'
    );

    $stmt->execute([
        ':received_at' => $commonInsertParams[':received_at'],
        ':request_method' => $commonInsertParams[':request_method'],
        ':remote_addr' => $commonInsertParams[':remote_addr'],
        ':user_agent' => $commonInsertParams[':user_agent'],
        ':authorization_header' => $commonInsertParams[':authorization_header'],
        ':content_type' => $commonInsertParams[':content_type'],
        ':event_type' => $commonInsertParams[':event_type'],
        ':status' => $status,
        ':status_raw' => $commonInsertParams[':status_raw'],
        ':transaction_id' => $commonInsertParams[':transaction_id'],
        ':charge_id' => $commonInsertParams[':charge_id'],
        ':store_id' => $commonInsertParams[':store_id'],
        ':customer_id' => $commonInsertParams[':customer_id'],
        ':amount' => $commonInsertParams[':amount'],
        ':currency' => $commonInsertParams[':currency'],
        ':livemode' => $commonInsertParams[':livemode'],
        ':source' => 'WEBHOOK',
        ':raw_json' => $commonInsertParams[':raw_json'],
    ]);

    send_json(200, [
        'ok' => true,
        'message' => 'Webhook received and saved.',
    ]);
} catch (Throwable $e) {
    write_error_log($e->getMessage());
    send_json(500, [
        'ok' => false,
        'message' => 'Internal Server Error',
    ]);
}
