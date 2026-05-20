<?php

require_once __DIR__ . '/lib/univapay_transactions.php';

function syncUnivapayAuthorized(array $univapayConfig): bool
{
    $expected = trim((string)($univapayConfig['fetch_endpoint_secret'] ?? ''));
    if ($expected === '') {
        return true;
    }

    $provided = univapayGetHeader('X-Univapay-Fetch-Secret');
    if ($provided === '') {
        $provided = trim((string)($_POST['secret'] ?? $_GET['secret'] ?? ''));
    }

    return hash_equals($expected, $provided);
}

function syncUnivapayRedirectUrl(string $status, array $params = []): string
{
    $returnTo = trim((string)($_POST['return_to'] ?? $_GET['return_to'] ?? 'index.php'));
    if ($returnTo === '') {
        $returnTo = 'index.php';
    }

    $allowed = ['index.php', 'nyukin/index.php', 'nyukin/payment_daily_dashboard.php', 'nyukin/mistake_finder.php'];
    $base = strtok($returnTo, '?') ?: 'index.php';
    if (!in_array($base, $allowed, true)) {
        $returnTo = 'index.php';
    }

    $parts = parse_url($returnTo);
    $path = (string)($parts['path'] ?? 'index.php');
    parse_str((string)($parts['query'] ?? ''), $query);
    unset($query['univapay_sync'], $query['univapay_count'], $query['univapay_saved'], $query['univapay_error']);
    $query = array_merge($query, ['univapay_sync' => $status], $params);

    return $path . '?' . http_build_query($query);
}

try {
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        header('Location: ' . syncUnivapayRedirectUrl('error', ['univapay_error' => 'method']));
        exit;
    }

    $config = univapayLoadConfig(__DIR__ . '/config.php');
    $univapayConfig = is_array($config['univapay'] ?? null) ? $config['univapay'] : [];

    if (!syncUnivapayAuthorized($univapayConfig)) {
        header('Location: ' . syncUnivapayRedirectUrl('error', ['univapay_error' => 'auth']));
        exit;
    }

    [$secret, $jwt] = univapayResolveCredentials($config);
    $startDate = trim((string)($_POST['start_date'] ?? gmdate('Y-m-01')));
    $endDate = trim((string)($_POST['end_date'] ?? gmdate('Y-m-d')));

    $result = univapayFetchAndStore($startDate, $endDate, [
        'secret' => $secret,
        'jwt' => $jwt,
        'base_url' => (string)($univapayConfig['api_base_url'] ?? UNIVAPAY_TRANSACTION_HISTORY_BASE_URL),
        'mode' => (string)($univapayConfig['mode'] ?? 'live'),
        'db_path' => __DIR__ . '/univapay_transaction_history.sqlite',
    ]);

    header('Location: ' . syncUnivapayRedirectUrl('success', [
        'univapay_count' => (string)$result['fetched_count'],
        'univapay_saved' => (string)$result['saved_count'],
    ]));
    exit;
} catch (Throwable $e) {
    header('Location: ' . syncUnivapayRedirectUrl('error', [
        'univapay_error' => substr($e->getMessage(), 0, 120),
    ]));
    exit;
}
