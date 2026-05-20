<?php

const UNIVAPAY_TRANSACTION_HISTORY_BASE_URL = 'https://api.univapay.com/transaction_history';

function univapayLoadConfig(string $configPath): array
{
    if (!file_exists($configPath)) {
        return [];
    }

    $config = require $configPath;
    return is_array($config) ? $config : [];
}

function univapayGetHeader(string $name): string
{
    $serverKey = 'HTTP_' . strtoupper(str_replace('-', '_', $name));
    if (isset($_SERVER[$serverKey])) {
        return trim((string)$_SERVER[$serverKey]);
    }

    return '';
}

function univapayResolveCredentials(array $config, ?string $fallbackSecret = null, ?string $fallbackJwt = null): array
{
    $univapayConfig = is_array($config['univapay'] ?? null) ? $config['univapay'] : [];

    $secret = getenv('UNIVAPAY_SECRET') ?: ($univapayConfig['secret'] ?? $fallbackSecret ?? '');
    $jwt = getenv('UNIVAPAY_JWT') ?: ($univapayConfig['jwt'] ?? $fallbackJwt ?? '');

    $secret = trim((string)$secret);
    $jwt = trim((string)$jwt);

    if ($secret === '' || $jwt === '') {
        throw new RuntimeException('UnivaPay API認証情報が設定されていません。');
    }

    return [$secret, $jwt];
}

function univapayDefaultDbPath(): string
{
    return dirname(__DIR__) . '/univapay_transaction_history.sqlite';
}

function univapayDateRangeToIso(string $startDate, string $endDate): array
{
    $timezone = new DateTimeZone('UTC');
    $start = DateTimeImmutable::createFromFormat('!Y-m-d', $startDate, $timezone);
    $end = DateTimeImmutable::createFromFormat('!Y-m-d', $endDate, $timezone);

    if (!$start || !$end) {
        throw new InvalidArgumentException('日付フォーマットが不正です。');
    }
    if ($start > $end) {
        throw new InvalidArgumentException('開始日は終了日以前を指定してください。');
    }

    return [
        $start->format('Y-m-d') . 'T00:00:00Z',
        $end->format('Y-m-d') . 'T23:59:59Z',
    ];
}

function univapayFetchTransactionHistory(string $from, string $to, array $options): array
{
    $secret = trim((string)($options['secret'] ?? ''));
    $jwt = trim((string)($options['jwt'] ?? ''));
    if ($secret === '' || $jwt === '') {
        throw new InvalidArgumentException('UnivaPay API認証情報が不足しています。');
    }

    if (!function_exists('curl_init')) {
        throw new RuntimeException('PHP cURL拡張が有効ではありません。');
    }

    $baseUrl = (string)($options['base_url'] ?? UNIVAPAY_TRANSACTION_HISTORY_BASE_URL);
    $mode = (string)($options['mode'] ?? 'live');
    $timeout = (int)($options['timeout'] ?? 30);
    $allItems = [];
    $cursor = null;
    $pageCount = 0;

    do {
        $params = [
            'from' => $from,
            'to' => $to,
            'mode' => $mode,
        ];
        if ($cursor) {
            $params['cursor'] = $cursor;
        }

        $url = $baseUrl . '?' . http_build_query($params);
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => [
                'Authorization: Bearer ' . $secret . '.' . $jwt,
                'Content-Type: application/json',
            ],
            CURLOPT_TIMEOUT => $timeout,
        ]);

        $response = curl_exec($ch);
        $curlError = curl_error($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if ($response === false) {
            throw new RuntimeException('curl error: ' . $curlError);
        }
        if ($httpCode < 200 || $httpCode >= 300) {
            throw new RuntimeException("HTTP ERROR: {$httpCode} / {$response}");
        }

        $data = json_decode((string)$response, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new RuntimeException('JSON decode error: ' . json_last_error_msg());
        }
        if (!is_array($data)) {
            throw new RuntimeException('UnivaPay APIレスポンスが不正です。');
        }

        foreach (($data['items'] ?? []) as $item) {
            if (is_array($item)) {
                $allItems[] = $item;
            }
        }

        $pageCount++;
        $hasMore = (bool)($data['has_more'] ?? false);
        $cursor = $data['next_cursor'] ?? null;
    } while ($hasMore && $cursor);

    return [
        'items' => $allItems,
        'page_count' => $pageCount,
    ];
}

function univapayCreatePdo(string $dbPath): PDO
{
    $pdo = new PDO('sqlite:' . $dbPath);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
    return $pdo;
}

function univapayEnsureTransactionHistoryTable(PDO $pdo): void
{
    $pdo->exec(
        'CREATE TABLE IF NOT EXISTS transaction_history (
            resource_id TEXT PRIMARY KEY,
            created_on TEXT,
            charge_id TEXT,
            type TEXT,
            status TEXT,
            result_kind TEXT,
            amount INTEGER,
            currency TEXT,
            payment_type TEXT,
            charge_type TEXT,
            bank_transfer_payment_status TEXT,
            bank_transfer_latest_deposit_date TEXT,
            cardholder_name TEXT,
            cardholder_email TEXT,
            brand TEXT,
            gateway TEXT,
            service_provider TEXT,
            metadata_name TEXT,
            metadata_phone_number TEXT,
            metadata_link_id TEXT,
            store_id TEXT,
            store_name TEXT,
            merchant_name TEXT,
            db_id TEXT,
            raw_json TEXT,
            updated_at TEXT
        )'
    );

    $columns = [];
    $tableInfo = $pdo->query('PRAGMA table_info(transaction_history)');
    if ($tableInfo !== false) {
        while ($column = $tableInfo->fetch(PDO::FETCH_ASSOC)) {
            $name = (string)($column['name'] ?? '');
            if ($name !== '') {
                $columns[$name] = true;
            }
        }
    }
    if (!isset($columns['result_kind'])) {
        $pdo->exec('ALTER TABLE transaction_history ADD COLUMN result_kind TEXT');
    }
}

function univapayEnsureWebhookEventsTable(PDO $pdo): void
{
    $pdo->exec(
        'CREATE TABLE IF NOT EXISTS univapay_webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT,
            resource_id TEXT,
            charge_id TEXT,
            detected_status TEXT,
            payload_sha256 TEXT UNIQUE,
            raw_json TEXT,
            received_at TEXT,
            created_at TEXT
        )'
    );
}

function univapayStatusPriority(?string $status): int
{
    return match ((string)$status) {
        'chargeback' => 100,
        default => 0,
    };
}

function univapayResultKindPriority(?string $resultKind): int
{
    return match ((string)$resultKind) {
        'chargeback' => 100,
        'refund' => 90,
        default => 0,
    };
}

function univapayWebhookStatusOverrideForTransaction(PDO $pdo, string $resourceId, ?string $chargeId): ?string
{
    try {
        univapayEnsureWebhookEventsTable($pdo);

        $conditions = ['resource_id = :resource_id'];
        $params = [':resource_id' => $resourceId];
        if ($chargeId !== null && $chargeId !== '') {
            $conditions[] = 'charge_id = :charge_id';
            $conditions[] = 'resource_id = :charge_id';
            $params[':charge_id'] = $chargeId;
        }

        $stmt = $pdo->prepare(
            'SELECT detected_status
             FROM univapay_webhook_events
             WHERE detected_status IS NOT NULL
               AND (' . implode(' OR ', $conditions) . ')
             ORDER BY id DESC
             LIMIT 1'
        );
        $stmt->execute($params);
        $status = $stmt->fetchColumn();

        return is_string($status) && $status !== '' ? $status : null;
    } catch (Throwable $e) {
        return null;
    }
}

function univapayEffectiveStatus(PDO $pdo, string $resourceId, ?string $incomingStatus, ?string $chargeId = null): ?string
{
    if ($incomingStatus !== null) {
        return $incomingStatus;
    }

    $stmt = $pdo->prepare('SELECT status FROM transaction_history WHERE resource_id = :resource_id');
    $stmt->execute([':resource_id' => $resourceId]);
    $existing = $stmt->fetchColumn();

    return is_string($existing) && $existing !== '' ? $existing : null;
}

function univapayNormalizeResultKind(?string $resultKind): ?string
{
    $resultKind = trim((string)$resultKind);
    if ($resultKind === '') {
        return null;
    }

    return match ($resultKind) {
        'chargeback', 'refund', 'transfer', 'payment' => $resultKind,
        'チャージバック' => 'chargeback',
        '返金' => 'refund',
        '振込' => 'transfer',
        '入金' => 'payment',
        default => $resultKind,
    };
}

function univapayWebhookResultOverrideForTransaction(PDO $pdo, string $resourceId, ?string $chargeId): ?string
{
    $status = univapayWebhookStatusOverrideForTransaction($pdo, $resourceId, $chargeId);
    return $status === 'chargeback' ? 'chargeback' : univapayNormalizeResultKind($status);
}

function univapayResultKindFromItem(array $item): string
{
    $type = trim((string)($item['type'] ?? ''));
    $paymentType = trim((string)($item['payment_type'] ?? ''));
    $rawJson = strtolower(json_encode($item, JSON_UNESCAPED_UNICODE) ?: '');

    if (str_contains($rawJson, 'chargeback') || str_contains($rawJson, 'dispute') || str_contains($rawJson, 'チャージバック')) {
        return 'chargeback';
    }
    if ($type === 'refund') {
        return 'refund';
    }
    if ($paymentType === 'bank_transfer') {
        return 'transfer';
    }

    return 'payment';
}

function univapayEffectiveResultKind(PDO $pdo, string $resourceId, ?string $incomingResultKind, ?string $chargeId = null): ?string
{
    $incomingResultKind = univapayNormalizeResultKind($incomingResultKind);
    $stmt = $pdo->prepare('SELECT result_kind FROM transaction_history WHERE resource_id = :resource_id');
    $stmt->execute([':resource_id' => $resourceId]);
    $existing = univapayNormalizeResultKind($stmt->fetchColumn() ?: null);

    if ($existing !== null && univapayResultKindPriority($existing) > univapayResultKindPriority($incomingResultKind)) {
        return $existing;
    }

    $webhookResultKind = univapayWebhookResultOverrideForTransaction($pdo, $resourceId, $chargeId);
    if ($webhookResultKind !== null && univapayResultKindPriority($webhookResultKind) > univapayResultKindPriority($incomingResultKind)) {
        return $webhookResultKind;
    }

    return $incomingResultKind;
}

function univapaySaveTransactionHistory(PDO $pdo, array $items): int
{
    univapayEnsureTransactionHistoryTable($pdo);

    $stmt = $pdo->prepare(
        'INSERT INTO transaction_history (
            resource_id, created_on, charge_id, type, status, result_kind, amount, currency,
            payment_type, charge_type, bank_transfer_payment_status, bank_transfer_latest_deposit_date,
            cardholder_name, cardholder_email, brand, gateway, service_provider,
            metadata_name, metadata_phone_number, metadata_link_id,
            store_id, store_name, merchant_name, db_id, raw_json, updated_at
        ) VALUES (
            :resource_id, :created_on, :charge_id, :type, :status, :result_kind, :amount, :currency,
            :payment_type, :charge_type, :bank_transfer_payment_status, :bank_transfer_latest_deposit_date,
            :cardholder_name, :cardholder_email, :brand, :gateway, :service_provider,
            :metadata_name, :metadata_phone_number, :metadata_link_id,
            :store_id, :store_name, :merchant_name, :db_id, :raw_json, :updated_at
        ) ON CONFLICT(resource_id) DO UPDATE SET
            created_on = excluded.created_on,
            charge_id = excluded.charge_id,
            type = excluded.type,
            status = excluded.status,
            result_kind = excluded.result_kind,
            amount = excluded.amount,
            currency = excluded.currency,
            payment_type = excluded.payment_type,
            charge_type = excluded.charge_type,
            bank_transfer_payment_status = excluded.bank_transfer_payment_status,
            bank_transfer_latest_deposit_date = excluded.bank_transfer_latest_deposit_date,
            cardholder_name = excluded.cardholder_name,
            cardholder_email = excluded.cardholder_email,
            brand = excluded.brand,
            gateway = excluded.gateway,
            service_provider = excluded.service_provider,
            metadata_name = excluded.metadata_name,
            metadata_phone_number = excluded.metadata_phone_number,
            metadata_link_id = excluded.metadata_link_id,
            store_id = excluded.store_id,
            store_name = excluded.store_name,
            merchant_name = excluded.merchant_name,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at'
    );

    $now = gmdate('c');
    $savedCount = 0;

    foreach ($items as $item) {
        if (!is_array($item)) {
            continue;
        }

        $metadata = is_array($item['metadata'] ?? null) ? $item['metadata'] : [];
        $userData = is_array($item['user_data'] ?? null) ? $item['user_data'] : [];
        $resourceId = $item['resource_id'] ?? null;
        if (!$resourceId) {
            continue;
        }

        $stmt->execute([
            ':resource_id' => $resourceId,
            ':created_on' => $item['created_on'] ?? null,
            ':charge_id' => $item['charge_id'] ?? null,
            ':type' => $item['type'] ?? null,
            ':status' => univapayEffectiveStatus(
                $pdo,
                (string)$resourceId,
                isset($item['status']) ? (string)$item['status'] : null,
                isset($item['charge_id']) ? (string)$item['charge_id'] : null
            ),
            ':result_kind' => univapayEffectiveResultKind(
                $pdo,
                (string)$resourceId,
                univapayResultKindFromItem($item),
                isset($item['charge_id']) ? (string)$item['charge_id'] : null
            ),
            ':amount' => $item['amount'] ?? null,
            ':currency' => $item['currency'] ?? null,
            ':payment_type' => $item['payment_type'] ?? null,
            ':charge_type' => $item['charge_type'] ?? null,
            ':bank_transfer_payment_status' => $item['bank_transfer_payment_status'] ?? null,
            ':bank_transfer_latest_deposit_date' => $item['bank_transfer_latest_deposit_date'] ?? null,
            ':cardholder_name' => $userData['cardholder_name'] ?? null,
            ':cardholder_email' => $userData['cardholder_email_address'] ?? null,
            ':brand' => $userData['brand'] ?? null,
            ':gateway' => $userData['gateway'] ?? null,
            ':service_provider' => $userData['service_provider'] ?? null,
            ':metadata_name' => $metadata['univapay-name'] ?? null,
            ':metadata_phone_number' => $metadata['univapay-phone-number'] ?? null,
            ':metadata_link_id' => $metadata['univapay-link-id'] ?? null,
            ':store_id' => $item['store_id'] ?? null,
            ':store_name' => $item['store_name'] ?? null,
            ':merchant_name' => $item['merchant_name'] ?? null,
            ':db_id' => null,
            ':raw_json' => json_encode($item, JSON_UNESCAPED_UNICODE),
            ':updated_at' => $now,
        ]);

        $savedCount++;
    }

    return $savedCount;
}

function univapayDetectWebhookStatus(array $payload): ?string
{
    $haystack = strtolower(json_encode($payload, JSON_UNESCAPED_UNICODE) ?: '');
    foreach (['chargeback', 'dispute', 'チャージバック'] as $needle) {
        if (str_contains($haystack, strtolower($needle))) {
            return 'chargeback';
        }
    }

    return null;
}

function univapayWebhookReferenceIds(array $payload): array
{
    $data = is_array($payload['data'] ?? null) ? $payload['data'] : [];
    $eventName = trim((string)($payload['event'] ?? $payload['trigger'] ?? $payload['type'] ?? ''));
    $eventResource = $eventName !== '' && strpos($eventName, '_') !== false
        ? substr($eventName, 0, strpos($eventName, '_'))
        : $eventName;
    $resourceId = trim((string)(
        $payload['resource_id']
        ?? $payload['transaction_id']
        ?? $data['resource_id']
        ?? $data['transaction_id']
        ?? ''
    ));
    $chargeId = trim((string)(
        $payload['charge_id']
        ?? $data['charge_id']
        ?? ($eventResource === 'charge' ? ($data['id'] ?? '') : '')
        ?? ''
    ));

    return [$resourceId, $chargeId];
}

function univapaySaveWebhookEvent(PDO $pdo, array $payload, string $rawPayload, DateTimeImmutable $receivedAt, string $eventName): ?string
{
    univapayEnsureTransactionHistoryTable($pdo);
    univapayEnsureWebhookEventsTable($pdo);

    $detectedStatus = univapayDetectWebhookStatus($payload);
    [$resourceId, $chargeId] = univapayWebhookReferenceIds($payload);
    $payloadHash = hash('sha256', $rawPayload);
    $now = gmdate('c');

    $stmt = $pdo->prepare(
        'INSERT OR IGNORE INTO univapay_webhook_events (
            event_name, resource_id, charge_id, detected_status, payload_sha256, raw_json, received_at, created_at
        ) VALUES (
            :event_name, :resource_id, :charge_id, :detected_status, :payload_sha256, :raw_json, :received_at, :created_at
        )'
    );
    $stmt->execute([
        ':event_name' => $eventName !== '' ? $eventName : null,
        ':resource_id' => $resourceId !== '' ? $resourceId : null,
        ':charge_id' => $chargeId !== '' ? $chargeId : null,
        ':detected_status' => $detectedStatus,
        ':payload_sha256' => $payloadHash,
        ':raw_json' => $rawPayload,
        ':received_at' => $receivedAt->format(DateTimeInterface::ATOM),
        ':created_at' => $now,
    ]);

    if ($detectedStatus === 'chargeback') {
        univapayApplyTransactionResultOverride($pdo, $detectedStatus, $resourceId, $chargeId, $now);
    }

    return $detectedStatus;
}

function univapayApplyTransactionResultOverride(PDO $pdo, string $resultKind, string $resourceId, string $chargeId, string $updatedAt): int
{
    $resultKind = univapayNormalizeResultKind($resultKind) ?? $resultKind;
    if ($resourceId === '' && $chargeId === '') {
        return 0;
    }

    if ($resourceId !== '') {
        $stmt = $pdo->prepare('UPDATE transaction_history SET result_kind = :result_kind, updated_at = :updated_at WHERE resource_id = :resource_id');
        $stmt->execute([':result_kind' => $resultKind, ':updated_at' => $updatedAt, ':resource_id' => $resourceId]);
        if ($stmt->rowCount() > 0) {
            return $stmt->rowCount();
        }
    }

    if ($chargeId !== '') {
        $stmt = $pdo->prepare('UPDATE transaction_history SET result_kind = :result_kind, updated_at = :updated_at WHERE charge_id = :charge_id OR resource_id = :charge_id');
        $stmt->execute([':result_kind' => $resultKind, ':updated_at' => $updatedAt, ':charge_id' => $chargeId]);
        return $stmt->rowCount();
    }

    return 0;
}

function univapayApplyTransactionStatusOverride(PDO $pdo, string $status, string $resourceId, string $chargeId, string $updatedAt): int
{
    if ($resourceId === '' && $chargeId === '') {
        return 0;
    }

    if ($resourceId !== '') {
        $stmt = $pdo->prepare('UPDATE transaction_history SET status = :status, updated_at = :updated_at WHERE resource_id = :resource_id');
        $stmt->execute([':status' => $status, ':updated_at' => $updatedAt, ':resource_id' => $resourceId]);
        if ($stmt->rowCount() > 0) {
            return $stmt->rowCount();
        }
    }

    if ($chargeId !== '') {
        $stmt = $pdo->prepare('UPDATE transaction_history SET status = :status, updated_at = :updated_at WHERE charge_id = :charge_id OR resource_id = :charge_id');
        $stmt->execute([':status' => $status, ':updated_at' => $updatedAt, ':charge_id' => $chargeId]);
        return $stmt->rowCount();
    }

    return 0;
}

function univapayFetchAndStore(string $startDate, string $endDate, array $options): array
{
    [$from, $to] = univapayDateRangeToIso($startDate, $endDate);

    $fetchResult = univapayFetchTransactionHistory($from, $to, $options);
    $dbPath = (string)($options['db_path'] ?? univapayDefaultDbPath());
    $pdo = univapayCreatePdo($dbPath);
    $savedCount = univapaySaveTransactionHistory($pdo, $fetchResult['items']);

    return [
        'start_date' => $startDate,
        'end_date' => $endDate,
        'from' => $from,
        'to' => $to,
        'fetched_count' => count($fetchResult['items']),
        'saved_count' => $savedCount,
        'page_count' => $fetchResult['page_count'],
        'db_path' => $dbPath,
    ];
}

function univapayWebhookFetchRange(DateTimeImmutable $receivedAt): array
{
    $end = $receivedAt->setTimezone(new DateTimeZone('UTC'));
    $start = $end->modify('first day of this month');

    return [
        $start->format('Y-m-d'),
        $end->format('Y-m-d'),
    ];
}
