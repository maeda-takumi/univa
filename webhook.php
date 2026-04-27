<?php
declare(strict_types=1);

const DB_DIR = __DIR__ . '/data';
const DB_FILE = DB_DIR . '/univapay_webhook.sqlite';
const LOG_FILE = DB_DIR . '/univapay_webhook_error.log';

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


function ensure_data_dir_exists(string $dir): void
{
    if (!is_dir($dir) && !mkdir($dir, 0777, true) && !is_dir($dir)) {
        throw new RuntimeException('dataフォルダの作成に失敗しました: ' . $dir);
    }
}

function write_error_log(string $message): void
{
    ensure_data_dir_exists(DB_DIR);

    file_put_contents(LOG_FILE, '[' . date('Y-m-d H:i:s') . '] ' . $message . PHP_EOL, FILE_APPEND);
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
            $headers[strtolower((string)$name)] = $value;
        }
    }
    return $headers;
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

function first_non_empty(array $values): ?string
{
    foreach ($values as $value) {
        if ($value !== null && trim((string)$value) !== '') {
            return trim((string)$value);
        }
    }
    return null;
}
function normalize_status(?string $raw): ?string
{
    if ($raw === null) {
        return null;
    }

    $normalized = strtolower(trim($raw));
    if ($normalized === '') {
        return null;
    }

    if (preg_match('/success|succeeded|completed|paid|captured|approved|成功|完了/u', $normalized) === 1) {
        return '成功';
    }

    if (preg_match('/pending|processing|in_progress|authorized|awaiting|処理中|保留/u', $normalized) === 1) {
        return '処理中';
    }

    if (preg_match('/refund|chargeback|reversed|返金|取消/u', $normalized) === 1) {
        return '返金/取消';
    }

    if (preg_match('/fail|cancel|error|expired|declined|voided|失敗|エラー|キャンセル/u', $normalized) === 1) {
        return '失敗';
    }

    return $raw;
}
function normalize_event(?string $raw): ?string
{
    if ($raw === null) {
        return null;
    }

    $normalized = strtolower(trim($raw));
    if ($normalized === '') {
        return null;
    }

    $direct = [
        'charge_finished' => '決済処理完了',
        'charge_pending' => '決済処理待ち',
        'charge_canceled' => 'キャンセル',
        'charge_cancelled' => 'キャンセル',
        'charge_refunded' => '返金処理完了',
        'chargeback_created' => 'チャージバック',
        'token_created' => 'トークン作成',
        'token_three_ds_updated' => '3Dセキュア状態更新',
    ];
    if (array_key_exists($normalized, $direct)) {
        return $direct[$normalized];
    }

    $keywordMappings = [
        [['three_ds', '3ds'], '3Dセキュア状態更新'],
        [['token'], 'トークン作成/更新'],
        [['chargeback'], 'チャージバック'],
        [['refund'], '返金処理完了'],
        [['cancel', 'canceled', 'cancelled', 'void'], 'キャンセル'],
        [['pending', 'processing'], '処理待ち'],
        [['failed', 'failure', 'error', 'decline'], '失敗'],
        [['payment', 'charge', 'capture'], '決済処理完了'],
    ];

    foreach ($keywordMappings as [$keywords, $jp]) {
        foreach ($keywords as $keyword) {
            if (str_contains($normalized, $keyword)) {
                return $jp;
            }
        }
    }

    return $raw;
}

function to_jst_datetime_string(?string $value): string
{

    $jst = new DateTimeZone('Asia/Tokyo');
    if ($value === null || trim($value) === '') {
        return (new DateTimeImmutable('now', $jst))->format('Y-m-d H:i:s');
    }

    $formats = ['Y/m/d H:i:s', 'Y-m-d H:i:s', DateTimeInterface::ATOM];
    foreach ($formats as $format) {
        $parsed = DateTimeImmutable::createFromFormat($format, trim($value));
        if ($parsed instanceof DateTimeImmutable) {
            return $parsed->setTimezone($jst)->format('Y-m-d H:i:s');
        }
    }

    try {
        return (new DateTimeImmutable(trim($value)))->setTimezone($jst)->format('Y-m-d H:i:s');
    } catch (Throwable) {
        return (new DateTimeImmutable('now', $jst))->format('Y-m-d H:i:s');
    }
}
function ensure_schema(PDO $pdo): void
{
    $pdo->exec(
        "CREATE TABLE IF NOT EXISTS csv_raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            imported_at TEXT NOT NULL,
            occurred_at_raw TEXT,
            event_type_raw TEXT,
            status_raw TEXT,
            transaction_id TEXT,
            charge_id TEXT,
            store_id TEXT,
            customer_ref TEXT,
            amount_raw TEXT,
            currency_raw TEXT,
            livemode_raw TEXT,
            raw_json TEXT NOT NULL
        )"
    );
    $pdo->exec(
        "CREATE TABLE IF NOT EXISTS webhook_raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            request_method TEXT,
            remote_addr TEXT,
            user_agent TEXT,
            authorization_header TEXT,
            content_type TEXT,
            event_type_raw TEXT,
            status_raw TEXT,
            transaction_id TEXT,
            charge_id TEXT,
            store_id TEXT,
            customer_ref TEXT,
            amount_raw TEXT,
            currency_raw TEXT,
            livemode_raw TEXT,
            payload_json TEXT NOT NULL
        )"
    );
    $pdo->exec(
        "CREATE TABLE IF NOT EXISTS payment_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL CHECK(source IN ('CSV', 'WEBHOOK')),
            source_event_id INTEGER,
            payment_date_jst TEXT NOT NULL,
            payer_name TEXT,
            amount INTEGER,
            email TEXT,
            event_type_norm TEXT,
            status_norm TEXT,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source, source_event_id, payment_date_jst)
        )"
    );
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_payment_facts_payment_date ON payment_facts(payment_date_jst)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_payment_facts_status ON payment_facts(status_norm)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_payment_facts_source ON payment_facts(source)');

    $pdo->exec('DROP VIEW IF EXISTS webhook_events');
    $pdo->exec('DROP TABLE IF EXISTS webhook_events');
    $pdo->exec(
        "CREATE VIEW webhook_events AS
        SELECT
            id,
            payment_date_jst AS received_at,
            payment_date_jst AS payment_date,
            NULL AS request_method,
            NULL AS remote_addr,
            NULL AS user_agent,
            NULL AS authorization_header,
            NULL AS content_type,
            event_type_norm AS event_type,
            status_norm AS status,
            NULL AS status_raw,
            NULL AS transaction_id,
            NULL AS charge_id,
            NULL AS store_id,
            NULL AS customer_id,
            amount,
            NULL AS currency,
            NULL AS livemode,
            payer_name,
            email,
            source,
            raw_json
        FROM payment_facts"
    );
}
try {
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        send_json(405, ['ok' => false, 'message' => 'POST only']);
    }

    $headers = get_request_headers_case_insensitive();
    $authorization = $headers['authorization'] ?? '';

    if (EXPECTED_AUTHORIZATION !== '' && $authorization !== EXPECTED_AUTHORIZATION) {
        send_json(401, ['ok' => false, 'message' => 'Unauthorized']);
    }

    $rawBody = file_get_contents('php://input');
    if ($rawBody === false || trim($rawBody) === '') {
        send_json(400, ['ok' => false, 'message' => 'Empty body']);
    }

    $payload = json_decode($rawBody, true);
    if (!is_array($payload)) {
        send_json(400, ['ok' => false, 'message' => 'Invalid JSON']);
    }

    ensure_data_dir_exists(DB_DIR);
    $pdo = new PDO('sqlite:' . DB_FILE);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
    ensure_schema($pdo);

    $eventTypeRaw = first_non_empty([
        $payload['event'] ?? null,
        $payload['event_type'] ?? null,
        $payload['type'] ?? null,
        get_nested($payload, ['data', 'event']),
    ]);

    $statusRaw = first_non_empty([
        get_nested($payload, ['data', 'status']),
        get_nested($payload, ['data', 'status']),
        get_nested($payload, ['data', 'three_ds', 'status']),
        get_nested($payload, ['data', 'data', 'three_ds', 'status']),
        get_nested($payload, ['transaction', 'status']),
        get_nested($payload, ['charge', 'status']),
    ]);
    $occurredRaw = first_non_empty([
        $payload['入金日'] ?? null,
        $payload['イベント作成日時'] ?? null,
        $payload['課金作成日時'] ?? null,
        get_nested($payload, ['data', 'captured_on']),
        get_nested($payload, ['data', 'paid_on']),
        get_nested($payload, ['data', 'created_on']),
        get_nested($payload, ['created_on']),
    ]);

    $storeId = first_non_empty([
        $payload['store_id'] ?? null,
        get_nested($payload, ['data', 'store_id']),
        get_nested($payload, ['store', 'id']),
    ]);

    $customerRef = first_non_empty([
        get_nested($payload, ['data', 'customer_id']),
        $payload['customer_id'] ?? null,
        get_nested($payload, ['customer', 'id']),
        get_nested($payload, ['data', 'email']),
    ]);

    $amountRaw = first_non_empty([
        (string)($payload['amount'] ?? ''),
        (string)(get_nested($payload, ['data', 'amount']) ?? ''),
        (string)(get_nested($payload, ['data', 'charged_amount']) ?? ''),
        (string)(get_nested($payload, ['data', 'requested_amount']) ?? ''),
    ]);
    $amount = null;
    if ($amountRaw !== null && is_numeric($amountRaw)) {
        $amount = (int)$amountRaw;
    }

    $currency = first_non_empty([
        $payload['currency'] ?? null,
        get_nested($payload, ['data', 'currency']),
        get_nested($payload, ['data', 'charged_currency']),
        get_nested($payload, ['data', 'requested_currency']),
    ]);

    $livemodeRaw = first_non_empty([
        is_bool($payload['livemode'] ?? null) ? (($payload['livemode'] ?? false) ? '1' : '0') : ($payload['livemode'] ?? null),
        get_nested($payload, ['data', 'livemode']),
    ]);

    $receivedAt = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s');
    $now = (new DateTimeImmutable('now', new DateTimeZone('Asia/Tokyo')))->format('Y-m-d H:i:s');

    $rawStmt = $pdo->prepare(
        'INSERT INTO webhook_raw_events (
            received_at, request_method, remote_addr, user_agent, authorization_header,
            content_type, event_type_raw, status_raw, transaction_id, charge_id,
            store_id, customer_ref, amount_raw, currency_raw, livemode_raw, payload_json
        ) VALUES (
            :received_at, :request_method, :remote_addr, :user_agent, :authorization_header,
            :content_type, :event_type_raw, :status_raw, :transaction_id, :charge_id,
            :store_id, :customer_ref, :amount_raw, :currency_raw, :livemode_raw, :payload_json
        )'
    );

    $rawStmt->execute([
        ':received_at' => $receivedAt,
        ':request_method' => $_SERVER['REQUEST_METHOD'] ?? null,
        ':remote_addr' => $_SERVER['REMOTE_ADDR'] ?? null,
        ':user_agent' => $_SERVER['HTTP_USER_AGENT'] ?? null,
        ':authorization_header' => $authorization !== '' ? $authorization : null,
        ':content_type' => $_SERVER['CONTENT_TYPE'] ?? null,
        ':event_type_raw' => $eventTypeRaw,
        ':status_raw' => $statusRaw,
        ':transaction_id' => first_non_empty([$payload['id'] ?? null, get_nested($payload, ['data', 'id'])]),
        ':charge_id' => first_non_empty([$payload['charge_id'] ?? null, get_nested($payload, ['data', 'charge_id'])]),
        ':store_id' => $storeId,
        ':customer_ref' => $customerRef,
        ':amount_raw' => $amountRaw,
        ':currency_raw' => $currency,
        ':livemode_raw' => $livemodeRaw,
        ':payload_json' => $rawBody,
    ]);

    $sourceEventId = (int)$pdo->lastInsertId();
    $payerName = first_non_empty([
        get_nested($payload, ['data', 'metadata', 'univapay-name']),
        get_nested($payload, ['data', 'metadata', 'name']),
        $payload['入金者名'] ?? null,
        $payload['氏名'] ?? null,
        $payload['カード名義'] ?? null,
    ]);
    $email = first_non_empty([
        get_nested($payload, ['data', 'email']),
        $payload['メールアドレス'] ?? null,
        get_nested($payload, ['customer', 'email']),
    ]);

    $factStmt = $pdo->prepare(
        'INSERT INTO payment_facts (
            source, source_event_id, payment_date_jst, payer_name, amount, email,
            event_type_norm, status_norm, raw_json, created_at, updated_at
        ) VALUES (
            :source, :source_event_id, :payment_date_jst, :payer_name, :amount, :email,
            :event_type_norm, :status_norm, :raw_json, :created_at, :updated_at
        ) ON CONFLICT(source, source_event_id, payment_date_jst) DO UPDATE SET
            payment_date_jst=excluded.payment_date_jst,
            payer_name=excluded.payer_name,
            amount=excluded.amount,
            email=excluded.email,
            event_type_norm=excluded.event_type_norm,
            status_norm=excluded.status_norm,
            raw_json=excluded.raw_json,
            updated_at=excluded.updated_at'
    );

    $factStmt->execute([
        ':source' => 'WEBHOOK',
        ':source_event_id' => $sourceEventId,
        ':payment_date_jst' => to_jst_datetime_string($occurredRaw ?? $receivedAt),
        ':payer_name' => $payerName,
        ':amount' => $amount,
        ':email' => $email,
        ':event_type_norm' => normalize_event($eventTypeRaw),
        ':status_norm' => normalize_status($statusRaw),
        ':raw_json' => $rawBody,
        ':created_at' => $now,
        ':updated_at' => $now,
    ]);

    send_json(200, ['ok' => true, 'message' => 'Webhook received and normalized.']);
} catch (Throwable $e) {
    write_error_log($e->getMessage());
    send_json(500, ['ok' => false, 'message' => 'Internal Server Error']);
}
