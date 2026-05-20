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
}

function univapaySaveTransactionHistory(PDO $pdo, array $items): int
{
    univapayEnsureTransactionHistoryTable($pdo);

    $stmt = $pdo->prepare(
        'INSERT INTO transaction_history (
            resource_id, created_on, charge_id, type, status, amount, currency,
            payment_type, charge_type, bank_transfer_payment_status, bank_transfer_latest_deposit_date,
            cardholder_name, cardholder_email, brand, gateway, service_provider,
            metadata_name, metadata_phone_number, metadata_link_id,
            store_id, store_name, merchant_name, db_id, raw_json, updated_at
        ) VALUES (
            :resource_id, :created_on, :charge_id, :type, :status, :amount, :currency,
            :payment_type, :charge_type, :bank_transfer_payment_status, :bank_transfer_latest_deposit_date,
            :cardholder_name, :cardholder_email, :brand, :gateway, :service_provider,
            :metadata_name, :metadata_phone_number, :metadata_link_id,
            :store_id, :store_name, :merchant_name, :db_id, :raw_json, :updated_at
        ) ON CONFLICT(resource_id) DO UPDATE SET
            created_on = excluded.created_on,
            charge_id = excluded.charge_id,
            type = excluded.type,
            status = excluded.status,
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
            ':status' => $item['status'] ?? null,
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
