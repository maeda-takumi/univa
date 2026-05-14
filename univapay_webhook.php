<?php

require_once __DIR__ . '/lib/univapay_transactions.php';

function univapayWebhookJsonResponse(int $statusCode, array $payload): void
{
    http_response_code($statusCode);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
}

function univapayWebhookDecodePayload(string $rawPayload): array
{
    if (trim($rawPayload) === '') {
        return [];
    }

    $payload = json_decode($rawPayload, true);
    if (json_last_error() !== JSON_ERROR_NONE || !is_array($payload)) {
        return [];
    }

    return $payload;
}

function univapayWebhookShouldFetchTransactions(array $payload): bool
{
    $event = (string)($payload['event'] ?? '');
    if ($event !== '' && strpos($event, 'token_') === 0) {
        return false;
    }

    $data = is_array($payload['data'] ?? null) ? $payload['data'] : [];
    $transactionId = (string)($payload['transaction_id'] ?? $data['transaction_id'] ?? $data['charge_id'] ?? '');
    if ($transactionId !== '') {
        return true;
    }

    return $event === '' || strpos($event, 'charge_') === 0 || strpos($event, 'transaction_') === 0;
}
function univapayWebhookAuthorized(array $univapayConfig): bool
{
    $expected = trim((string)($univapayConfig['webhook_secret'] ?? ''));
    if ($expected === '') {
        return true;
    }

    $provided = univapayGetHeader('X-Univapay-Webhook-Secret');
    if ($provided === '') {
        $provided = univapayGetHeader('X-Webhook-Secret');
    }
    if ($provided === '') {
        $provided = trim((string)($_GET['secret'] ?? ''));
    }

    return hash_equals($expected, $provided);
}

try {
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        univapayWebhookJsonResponse(405, [
            'ok' => false,
            'error' => 'POSTメソッドで実行してください。',
        ]);
        return;
    }

    $receivedAt = new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $rawPayload = (string)file_get_contents('php://input');
    $payload = univapayWebhookDecodePayload($rawPayload);
    $config = univapayLoadConfig(__DIR__ . '/config.php');
    $univapayConfig = is_array($config['univapay'] ?? null) ? $config['univapay'] : [];

    if (!univapayWebhookAuthorized($univapayConfig)) {
        univapayWebhookJsonResponse(403, [
            'ok' => false,
            'error' => 'Webhookシークレットが不正です。',
        ]);
        return;
    }

    if (!univapayWebhookShouldFetchTransactions($payload)) {
        univapayWebhookJsonResponse(200, [
            'ok' => true,
            'message' => '取引履歴の再取得が不要なWebhookイベントのため受信のみ完了しました。',
            'received_at' => $receivedAt->format(DateTimeInterface::ATOM),
            'event' => $payload['event'] ?? null,
            'payload_sha256' => hash('sha256', $rawPayload),
            'payload_used_as_business_data' => false,
            'skipped_fetch' => true,
        ]);
        return;
    }

    [$secret, $jwt] = univapayResolveCredentials($config);
    $fetchDays = (int)($univapayConfig['webhook_fetch_days'] ?? 1);
    [$startDate, $endDate] = univapayWebhookFetchRange($receivedAt, $fetchDays);

    // Webhook payloadは業務データとして使わず、受信日時をトリガーにして既存の取引履歴APIから取得します。
    $result = univapayFetchAndStore($startDate, $endDate, [
        'secret' => $secret,
        'jwt' => $jwt,
        'base_url' => (string)($univapayConfig['api_base_url'] ?? UNIVAPAY_TRANSACTION_HISTORY_BASE_URL),
        'mode' => (string)($univapayConfig['mode'] ?? 'live'),
        'db_path' => __DIR__ . '/univapay_transaction_history.sqlite',
    ]);

    univapayWebhookJsonResponse(200, [
        'ok' => true,
        'message' => 'WebhookをトリガーにUnivaPay API取得とDB保存が完了しました。',
        'received_at' => $receivedAt->format(DateTimeInterface::ATOM),
        'event' => $payload['event'] ?? null,
        'payload_sha256' => hash('sha256', $rawPayload),
        'payload_used_as_business_data' => false,
        'skipped_fetch' => false,
        'result' => $result,
    ]);
} catch (Throwable $e) {
    univapayWebhookJsonResponse(500, [
        'ok' => false,
        'error' => $e->getMessage(),
    ]);
}
