<?php

$secret = '9X9sL29YItHjZgYvK553';
$jwt    = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhcHBfdG9rZW4iLCJpYXQiOjE3Nzc2MTE5ODAsIm1lcmNoYW50X2lkIjoiMTFmMDgzNWYtMjBmYi1kNWEyLWE0NzEtZGY2YzExOTgwNmNkIiwic3RvcmVfaWQiOiIxMWYwODM1Zi0yMjc4LWFkMDYtOWFlYy01M2I1ZjYxZWFjZTYiLCJkb21haW5zIjpbXSwibW9kZSI6ImxpdmUiLCJjcmVhdG9yX2lkIjoiMTFmMDgzNWYtMjBmYi1kNWEyLWE0NzEtZGY2YzExOTgwNmNkIiwidmVyc2lvbiI6MSwianRpIjoiMTFmMTQ1MWItN2RiZC1jZWFjLWIxZDAtOTdhNjc4NGEwNTVhIn0.0q25f34nwO2YFI7Jqkr6SAkMbQodke_NIBX41G8oXFM';


$baseUrl = 'https://api.univapay.com/transaction_history';

$dbPath = __DIR__ . '/univapay_transaction_history.sqlite';

$startDate = $_POST['start_date'] ?? gmdate('Y-m-01');
$endDate = $_POST['end_date'] ?? gmdate('Y-m-d');
$message = null;
$error = null;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $start = DateTime::createFromFormat('Y-m-d', $startDate, new DateTimeZone('UTC'));
    $end = DateTime::createFromFormat('Y-m-d', $endDate, new DateTimeZone('UTC'));

    if (!$start || !$end) {
        $error = '日付フォーマットが不正です。';
    } elseif ($start > $end) {
        $error = '開始日は終了日以前を指定してください。';
    } else {
        $from = $start->format('Y-m-d') . 'T00:00:00Z';
        $to = $end->format('Y-m-d') . 'T23:59:59Z';

        $allItems = [];
        $cursor = null;

        do {
            $params = [
                'from' => $from,
                'to' => $to,
                'mode' => 'live',
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
                CURLOPT_TIMEOUT => 30,
            ]);

            $response = curl_exec($ch);
            $curlError = curl_error($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            if ($response === false) {
                $error = 'curl error: ' . $curlError;
                break;
            }

            if ($httpCode < 200 || $httpCode >= 300) {
                $error = "HTTP ERROR: {$httpCode} / {$response}";
                break;
            }

            $data = json_decode($response, true);

            if (json_last_error() !== JSON_ERROR_NONE) {
                $error = 'JSON decode error: ' . json_last_error_msg();
                break;
            }

            foreach (($data['items'] ?? []) as $item) {
                $allItems[] = $item;
            }

            $hasMore = $data['has_more'] ?? false;
            $cursor = $data['next_cursor'] ?? null;
        } while ($hasMore && $cursor);

        if (!$error) {
            try {
                $pdo = new PDO('sqlite:' . $dbPath);
                $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

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
                        raw_json TEXT,
                        updated_at TEXT
                    )'
                );

                $stmt = $pdo->prepare(
                    'INSERT INTO transaction_history (
                        resource_id, created_on, charge_id, type, status, amount, currency,
                        payment_type, charge_type, bank_transfer_payment_status, bank_transfer_latest_deposit_date,
                        cardholder_name, cardholder_email, brand, gateway, service_provider,
                        metadata_name, metadata_phone_number, metadata_link_id,
                        store_id, store_name, merchant_name, raw_json, updated_at
                    ) VALUES (
                        :resource_id, :created_on, :charge_id, :type, :status, :amount, :currency,
                        :payment_type, :charge_type, :bank_transfer_payment_status, :bank_transfer_latest_deposit_date,
                        :cardholder_name, :cardholder_email, :brand, :gateway, :service_provider,
                        :metadata_name, :metadata_phone_number, :metadata_link_id,
                        :store_id, :store_name, :merchant_name, :raw_json, :updated_at
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

                foreach ($allItems as $item) {
                    $metadata = $item['metadata'] ?? [];
                    $userData = $item['user_data'] ?? [];

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
                        ':raw_json' => json_encode($item, JSON_UNESCAPED_UNICODE),
                        ':updated_at' => $now,
                    ]);

                    $savedCount++;
                }

                $message = "DB保存完了: {$savedCount}件 / 取得件数: " . count($allItems) . "件 / 保存先: {$dbPath}";
            } catch (PDOException $e) {
                $error = 'DBエラー: ' . $e->getMessage();
            }
        }
    }
}

?>
<!doctype html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>UnivaPay 取引履歴取得</title>
</head>
<body>
    <h1>UnivaPay 取引履歴取得</h1>

    <form method="post">
        <label>
            取得期間(開始)
            <input type="date" name="start_date" value="<?= htmlspecialchars($startDate, ENT_QUOTES, 'UTF-8') ?>" required>
        </label>
        <br>
        <label>
            取得期間(終了)
            <input type="date" name="end_date" value="<?= htmlspecialchars($endDate, ENT_QUOTES, 'UTF-8') ?>" required>
        </label>
        <br><br>
        <button type="submit">実行</button>
    </form>

    <?php if ($message): ?>
        <p style="color: green;"><?= htmlspecialchars($message, ENT_QUOTES, 'UTF-8') ?></p>
    <?php endif; ?>

    <?php if ($error): ?>
        <p style="color: red;"><?= htmlspecialchars($error, ENT_QUOTES, 'UTF-8') ?></p>
    <?php endif; ?>
</body>
</html>
