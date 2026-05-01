<?php
// export_univapay_transaction_history.php

$secret = '9X9sL29YItHjZgYvK553';
$jwt    = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhcHBfdG9rZW4iLCJpYXQiOjE3Nzc2MTE5ODAsIm1lcmNoYW50X2lkIjoiMTFmMDgzNWYtMjBmYi1kNWEyLWE0NzEtZGY2YzExOTgwNmNkIiwic3RvcmVfaWQiOiIxMWYwODM1Zi0yMjc4LWFkMDYtOWFlYy01M2I1ZjYxZWFjZTYiLCJkb21haW5zIjpbXSwibW9kZSI6ImxpdmUiLCJjcmVhdG9yX2lkIjoiMTFmMDgzNWYtMjBmYi1kNWEyLWE0NzEtZGY2YzExOTgwNmNkIiwidmVyc2lvbiI6MSwianRpIjoiMTFmMTQ1MWItN2RiZC1jZWFjLWIxZDAtOTdhNjc4NGEwNTVhIn0.0q25f34nwO2YFI7Jqkr6SAkMbQodke_NIBX41G8oXFM';


$baseUrl = 'https://api.univapay.com/transaction_history';

$from = '2026-04-01T00:00:00Z';
$to   = '2026-05-01T00:00:00Z';

$allItems = [];
$cursor = null;

do {
    $params = [
        'from' => $from,
        'to'   => $to,
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
        exit('curl error: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode >= 300) {
        echo "HTTP ERROR: {$httpCode}\n";
        echo $response;
        exit;
    }

    $data = json_decode($response, true);

    if (json_last_error() !== JSON_ERROR_NONE) {
        exit('JSON decode error: ' . json_last_error_msg());
    }

    foreach (($data['items'] ?? []) as $item) {
        $allItems[] = $item;
    }

    $hasMore = $data['has_more'] ?? false;
    $cursor  = $data['next_cursor'] ?? null;

} while ($hasMore && $cursor);

// CSV保存先
$csvPath = __DIR__ . '/univapay_transaction_history.csv';

$fp = fopen($csvPath, 'w');

if (!$fp) {
    exit('CSVファイルを作成できませんでした');
}

// Excel文字化け対策
fwrite($fp, "\xEF\xBB\xBF");

// ヘッダー
fputcsv($fp, [
    'created_on',
    'resource_id',
    'charge_id',
    'type',
    'status',
    'amount',
    'currency',
    'payment_type',
    'charge_type',
    'bank_transfer_payment_status',
    'bank_transfer_latest_deposit_date',
    'cardholder_name',
    'cardholder_email',
    'brand',
    'gateway',
    'service_provider',
    'metadata_name',
    'metadata_phone_number',
    'metadata_link_id',
    'store_id',
    'store_name',
    'merchant_name',
]);

foreach ($allItems as $item) {
    $metadata = $item['metadata'] ?? [];
    $userData = $item['user_data'] ?? [];

    fputcsv($fp, [
        $item['created_on'] ?? '',
        $item['resource_id'] ?? '',
        $item['charge_id'] ?? '',
        $item['type'] ?? '',
        $item['status'] ?? '',
        $item['amount'] ?? '',
        $item['currency'] ?? '',
        $item['payment_type'] ?? '',
        $item['charge_type'] ?? '',
        $item['bank_transfer_payment_status'] ?? '',
        $item['bank_transfer_latest_deposit_date'] ?? '',
        $userData['cardholder_name'] ?? '',
        $userData['cardholder_email_address'] ?? '',
        $userData['brand'] ?? '',
        $userData['gateway'] ?? '',
        $userData['service_provider'] ?? '',
        $metadata['univapay-name'] ?? '',
        $metadata['univapay-phone-number'] ?? '',
        $metadata['univapay-link-id'] ?? '',
        $item['store_id'] ?? '',
        $item['store_name'] ?? '',
        $item['merchant_name'] ?? '',
    ]);
}

fclose($fp);

header('Content-Type: text/plain; charset=utf-8');

echo "CSV作成完了\n";
echo "取得件数: " . count($allItems) . "\n";
echo "保存先: " . $csvPath . "\n";