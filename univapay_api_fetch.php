<?php

require_once __DIR__ . '/lib/univapay_transactions.php';

function univapayApiFetchJsonResponse(int $statusCode, array $payload): void
{
    http_response_code($statusCode);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
}

function univapayApiFetchAuthorized(array $univapayConfig): bool
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

try {
    if (PHP_SAPI !== 'cli' && $_SERVER['REQUEST_METHOD'] !== 'POST') {
        univapayApiFetchJsonResponse(405, [
            'ok' => false,
            'error' => 'POSTメソッドで実行してください。',
        ]);
        return;
    }

    $config = univapayLoadConfig(__DIR__ . '/config.php');
    $univapayConfig = is_array($config['univapay'] ?? null) ? $config['univapay'] : [];

    if (PHP_SAPI !== 'cli' && !univapayApiFetchAuthorized($univapayConfig)) {
        univapayApiFetchJsonResponse(403, [
            'ok' => false,
            'error' => 'API実行用シークレットが不正です。',
        ]);
        return;
    }

    [$secret, $jwt] = univapayResolveCredentials($config);
    $today = gmdate('Y-m-d');
    $startDate = trim((string)($_POST['start_date'] ?? $_GET['start_date'] ?? $today));
    $endDate = trim((string)($_POST['end_date'] ?? $_GET['end_date'] ?? $startDate));

    $result = univapayFetchAndStore($startDate, $endDate, [
        'secret' => $secret,
        'jwt' => $jwt,
        'base_url' => (string)($univapayConfig['api_base_url'] ?? UNIVAPAY_TRANSACTION_HISTORY_BASE_URL),
        'mode' => (string)($univapayConfig['mode'] ?? 'live'),
        'db_path' => __DIR__ . '/univapay_transaction_history.sqlite',
    ]);

    univapayApiFetchJsonResponse(200, [
        'ok' => true,
        'message' => 'UnivaPay API取得とDB保存が完了しました。',
        'result' => $result,
    ]);
} catch (Throwable $e) {
    univapayApiFetchJsonResponse(500, [
        'ok' => false,
        'error' => $e->getMessage(),
    ]);
}
