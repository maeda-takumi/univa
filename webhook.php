<?php
declare(strict_types=1);

const DB_DIR = __DIR__ . '/data';
const DB_FILE = DB_DIR . '/payments.sqlite';

const EXPECTED_AUTHORIZATION = '';

const LOG_FILE = DB_DIR . '/webhook.log';

function write_log(string $level, string $message, array $context = []): void
{
    if (!is_dir(DB_DIR)) {
        mkdir(DB_DIR, 0775, true);
    }

    $record = [
        'time' => now_utc(),
        'level' => $level,
        'message' => $message,
        'context' => $context,
    ];

    $line = json_encode($record, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if ($line === false) {
        $line = sprintf('{"time":"%s","level":"error","message":"log_encode_failed"}', now_utc());
    }

    file_put_contents(LOG_FILE, $line . PHP_EOL, FILE_APPEND | LOCK_EX);
}

function send_json(int $status, array $payload): void
{
    http_response_code($status);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

function ensure_schema(PDO $pdo): void
{
    $pdo->exec(
        "CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            amount INTEGER,
            email TEXT,
            payer_name TEXT,
            current_status TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_event_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )"
    );
    $pdo->exec(
        "CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT NOT NULL,
            provider_event_id TEXT,
            event_type TEXT,
            status TEXT,
            occurred_at TEXT,
            received_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_hash TEXT,
            is_duplicate INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
        )"
    );
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_events_transaction_id ON webhook_events(transaction_id)');
    $pdo->exec('CREATE INDEX IF NOT EXISTS idx_webhook_events_occurred_at ON webhook_events(occurred_at)');
    $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_events_provider_event_id ON webhook_events(provider_event_id) WHERE provider_event_id IS NOT NULL');
    $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_events_payload_hash ON webhook_events(payload_hash) WHERE payload_hash IS NOT NULL');
}

function get_nested(array $payload, array $keys): mixed
{
    $value = $payload;
    foreach ($keys as $key) {
        if (!is_array($value) || !array_key_exists($key, $value)) {
            return null;
        }
        $value = $value[$key];
    }
    return $value;
}

function first_non_empty(array $values): ?string
{
    foreach ($values as $value) {
        if ($value === null) {
            continue;
        }
        $s = trim((string)$value);
        if ($s !== '') {
            return $s;
        }
    }
    return null;
}
function pick_from_child_json_first(array $payload, array $childJsonPaths, array $fallbackValues = []): ?string
{
    $childValues = [];
    foreach ($childJsonPaths as $path) {
        $childValues[] = get_nested($payload, $path);
    }

    return first_non_empty(array_merge($childValues, $fallbackValues));
}

function now_utc(): string
{
    return (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s');
}

function now_jst(): string
{
    return (new DateTimeImmutable('now', new DateTimeZone('Asia/Tokyo')))->format('Y-m-d H:i:s');
}

function to_jst_datetime(?string $value): ?string
{
    if ($value === null) {
        return null;
    }

    $trimmed = trim($value);
    if ($trimmed === '') {
        return null;
    }

    try {
        if (ctype_digit($trimmed)) {
            $dt = (new DateTimeImmutable('@' . $trimmed))->setTimezone(new DateTimeZone('Asia/Tokyo'));
            return $dt->format('Y-m-d H:i:s');
        }

        $dt = (new DateTimeImmutable($trimmed, new DateTimeZone('UTC')))->setTimezone(new DateTimeZone('Asia/Tokyo'));
        return $dt->format('Y-m-d H:i:s');
    } catch (Throwable) {
        return $value;
    }
}

function to_japanese_status(?string $status): ?string
{
    if ($status === null) {
        return null;
    }

    $normalized = strtolower(trim($status));
    $map = [
        'pending' => '保留',
        'authorized' => '与信済み',
        'captured' => '売上確定',
        'succeeded' => '成功',
        'failed' => '失敗',
        'error' => 'エラー',
        'canceled' => 'キャンセル',
        'cancelled' => 'キャンセル',
        'refunded' => '返金済み',
        'chargeback' => 'チャージバック',
        'unknown' => '不明',
    ];

    return $map[$normalized] ?? $status;
}

function to_japanese_event_type(?string $eventType): ?string
{
    if ($eventType === null) {
        return null;
    }

    $normalized = strtolower(trim($eventType));
    $map = [
        'payment.created' => '決済作成',
        'payment.updated' => '決済更新',
        'payment.succeeded' => '決済成功',
        'payment.failed' => '決済失敗',
        'payment.canceled' => '決済キャンセル',
        'payment.cancelled' => '決済キャンセル',
        'payment.refunded' => '返金完了',
    ];

    return $map[$normalized] ?? $eventType;
}

if (!is_dir(DB_DIR)) {
    mkdir(DB_DIR, 0775, true);
}

try {
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        write_log('warning', 'invalid_method', ['method' => $_SERVER['REQUEST_METHOD'] ?? null]);
        send_json(405, ['ok' => false, 'message' => 'POST only']);
    }

    $headers = getallheaders() ?: [];
    $auth = $headers['Authorization'] ?? $headers['authorization'] ?? '';
    if (EXPECTED_AUTHORIZATION !== '' && $auth !== EXPECTED_AUTHORIZATION) {
        write_log('warning', 'unauthorized', ['has_auth_header' => $auth !== '']);
        send_json(401, ['ok' => false, 'message' => 'Unauthorized']);
    }

    $rawBody = file_get_contents('php://input');
    if ($rawBody === false || trim($rawBody) === '') {
        write_log('warning', 'invalid_json', ['raw_body' => $rawBody]);
        send_json(400, ['ok' => false, 'message' => 'Empty body']);
    }

    $payload = json_decode($rawBody, true);
    if (!is_array($payload)) {
        write_log('warning', 'invalid_json', ['raw_body' => $rawBody]);
        send_json(400, ['ok' => false, 'message' => 'Invalid JSON']);
    }

    $transactionId = first_non_empty([
        get_nested($payload, ['data', 'id']),
        $payload['id'] ?? null,
        get_nested($payload, ['transaction', 'id'])
    ]);

    if ($transactionId === null) {
        write_log('warning', 'transaction_id_not_found', ['payload' => $payload]);
        send_json(400, ['ok' => false, 'message' => 'transaction_id not found']);
    }

    $statusRaw = pick_from_child_json_first(
        $payload,
        [
            ['data', 'status'],
            ['transaction', 'status'],
        ],
        [$payload['status'] ?? null]
    ) ?? 'unknown';
    $status = to_japanese_status($statusRaw) ?? '不明';

    $eventTypeRaw = first_non_empty([
        $payload['event'] ?? null,
        get_nested($payload, ['type']),
        get_nested($payload, ['data', 'event'])
    ]);
    
    $eventType = to_japanese_event_type($eventTypeRaw);

    $occurredAtRaw = first_non_empty([
        get_nested($payload, ['data', 'created_on']),
        get_nested($payload, ['created_on']),
        get_nested($payload, ['occurred_at'])
    ]);

    $occurredAt = to_jst_datetime($occurredAtRaw);
    $amountRaw = pick_from_child_json_first(
        $payload,
        [
            ['data', 'amount'],
            ['data', 'charged_amount'],
        ],
        [$payload['amount'] ?? null]
    );
    $amount = ($amountRaw !== null && is_numeric($amountRaw)) ? (int)$amountRaw : null;

    $email = pick_from_child_json_first(
        $payload,
        [
            ['data', 'email'],
            ['customer', 'email'],
        ],
        [$payload['email'] ?? null]
    );

    $payerName = pick_from_child_json_first(
        $payload,
        [
            ['data', 'metadata', 'univapay-name'],
            ['data', 'metadata', 'name'],
            ['customer', 'name'],
        ],
        [$payload['name'] ?? null]
    );
    
    $providerEventId = first_non_empty([
        $payload['id'] ?? null,
        get_nested($payload, ['event_id'])
    ]);

    $receivedAt = now_jst();
    $payloadHash = hash('sha256', $rawBody);

    $pdo = new PDO('sqlite:' . DB_FILE);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->exec('PRAGMA foreign_keys = ON');
    ensure_schema($pdo);

    $pdo->beginTransaction();

    $dup = 0;
    $dupStmt = $pdo->prepare('SELECT id FROM webhook_events WHERE provider_event_id = :provider_event_id OR payload_hash = :payload_hash LIMIT 1');
    $dupStmt->execute([':provider_event_id' => $providerEventId, ':payload_hash' => $payloadHash]);
    if ($dupStmt->fetchColumn() !== false) {
        $dup = 1;
    }
    $upsertTransaction = $pdo->prepare(
        'INSERT INTO transactions (
            transaction_id, amount, email, payer_name, current_status,
            first_seen_at, last_seen_at, last_event_at, created_at, updated_at

        ) VALUES (
            :transaction_id, :amount, :email, :payer_name, :current_status,
            :first_seen_at, :last_seen_at, :last_event_at, :created_at, :updated_at
        )
        ON CONFLICT(transaction_id) DO UPDATE SET
            amount = COALESCE(excluded.amount, transactions.amount),
            email = COALESCE(excluded.email, transactions.email),
            payer_name = COALESCE(excluded.payer_name, transactions.payer_name),
            current_status = excluded.current_status,
            last_seen_at = excluded.last_seen_at,
            last_event_at = COALESCE(excluded.last_event_at, transactions.last_event_at),
            updated_at = excluded.updated_at'
    );


    $upsertTransaction->execute([
        ':transaction_id' => $transactionId,
        ':amount' => $amount,
        ':email' => $email,
        ':payer_name' => $payerName,
        ':current_status' => $status,
        ':first_seen_at' => $receivedAt,
        ':last_seen_at' => $receivedAt,
        ':last_event_at' => $occurredAt,
        ':created_at' => $receivedAt,
        ':updated_at' => $receivedAt,
    ]);

    $insertEvent = $pdo->prepare(
        'INSERT OR IGNORE INTO webhook_events (
            transaction_id, provider_event_id, event_type, status, occurred_at,
            received_at, payload_json, payload_hash, is_duplicate, created_at
        ) VALUES (
            :transaction_id, :provider_event_id, :event_type, :status, :occurred_at,
            :received_at, :payload_json, :payload_hash, :is_duplicate, :created_at
        )'
    );

    $insertEvent->execute([
        ':transaction_id' => $transactionId,
        ':provider_event_id' => $providerEventId,
        ':event_type' => $eventType,
        ':status' => $status,
        ':occurred_at' => $occurredAt,
        ':received_at' => $receivedAt,
        ':payload_json' => $rawBody,
        ':payload_hash' => $payloadHash,
        ':is_duplicate' => $dup,
        ':created_at' => $receivedAt,
    ]);

    if ($insertEvent->rowCount() === 0) {
        $dup = 1;
    }

    $pdo->commit();

    write_log('info', 'webhook_processed', [
        'transaction_id' => $transactionId,
        'status' => $status,
        'event_type' => $eventType,
        'provider_event_id' => $providerEventId,
        'is_duplicate' => $dup === 1,
    ]);

    send_json(200, [
        'ok' => true,
        'transaction_id' => $transactionId,
        'status' => $status,
        'duplicate' => $dup === 1,
    ]);
} catch (Throwable $e) {
    if (isset($pdo) && $pdo instanceof PDO && $pdo->inTransaction()) {
        $pdo->rollBack();
    }
    write_log('error', 'webhook_processing_failed', [
        'error' => $e->getMessage(),
        'trace' => $e->getTraceAsString(),
    ]);
    send_json(500, ['ok' => false, 'message' => 'Internal Server Error', 'error' => $e->getMessage()]);
}
