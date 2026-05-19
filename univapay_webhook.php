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
    if (json_last_error() !== JSON_ERROR_NONE) {
        return [];
    }

    if (is_string($payload)) {
        $nestedPayload = json_decode($payload, true);
        if (json_last_error() === JSON_ERROR_NONE && is_array($nestedPayload)) {
            return $nestedPayload;
        }

        return [];
    }

    return is_array($payload) ? $payload : [];
}

function univapayWebhookEventName(array $payload): string
{
    $event = trim((string)($payload['event'] ?? $payload['trigger'] ?? $payload['type'] ?? ''));
    if ($event !== '') {
        return $event;
    }

    $data = is_array($payload['data'] ?? null) ? $payload['data'] : [];
    return trim((string)($data['event'] ?? $data['trigger'] ?? ''));
}

function univapayWebhookData(array $payload): array
{
    return is_array($payload['data'] ?? null) ? $payload['data'] : [];
}

function univapayWebhookEventResource(array $payload): string
{
    $event = univapayWebhookEventName($payload);
    if ($event === '') {
        return '';
    }

    $separatorPosition = strpos($event, '_');
    return $separatorPosition === false ? $event : substr($event, 0, $separatorPosition);
}

function univapayWebhookTransactionReference(array $payload): string
{
    $data = univapayWebhookData($payload);
    return trim((string)(
        $payload['transaction_id']
        ?? $data['transaction_id']
        ?? $payload['charge_id']
        ?? $data['charge_id']
        ?? ''
    ));
}

function univapayWebhookSkipFetchReason(array $payload): ?string
{
    $resource = univapayWebhookEventResource($payload);
    if ($resource === 'token') {
        return 'token_event_without_transaction_history';
    }

    $data = univapayWebhookData($payload);
    $tokenId = (string)($payload['token_id'] ?? $data['token_id'] ?? $data['id'] ?? '');
    $type = (string)($payload['type'] ?? $data['type'] ?? '');
    if ($tokenId !== '' && in_array($type, ['one_time', 'recurring'], true)) {
        return 'payment_token_without_transaction_history';
    }

    return null;
}

function univapayWebhookShouldFetchTransactions(array $payload): bool
{
    if (univapayWebhookSkipFetchReason($payload) !== null) {
        return false;
    }

    if (univapayWebhookTransactionReference($payload) !== '') {
        return true;
    }

    // UnivaPay の webhook は charge/refund/subscription など複数のトリガーがあり、
    // 取引履歴 API は課金・返金を横断して取得できるため、token 系以外は受信を
    // トリガーとして必ず再取得します。
    return true;
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
function univapayHandleWebhookRequest(?string $rawPayload = null, ?DateTimeImmutable $receivedAt = null): void
{
    try {
        if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
            univapayWebhookJsonResponse(405, [
                'ok' => false,
                'error' => 'POSTメソッドで実行してください。',
            ]);
            return;
        }

        $receivedAt = $receivedAt ?? new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $rawPayload = $rawPayload ?? (string)file_get_contents('php://input');
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

        $skipReason = univapayWebhookSkipFetchReason($payload);
        if (!univapayWebhookShouldFetchTransactions($payload)) {
            univapayWebhookJsonResponse(200, [
                'ok' => true,
                'message' => '取引履歴の再取得が不要なWebhookイベントのため受信のみ完了しました。',
                'received_at' => $receivedAt->format(DateTimeInterface::ATOM),
                'event' => univapayWebhookEventName($payload) ?: null,
                'payload_sha256' => hash('sha256', $rawPayload),
                'payload_used_as_business_data' => false,
                'skipped_fetch' => true,
                'skip_reason' => $skipReason ?? 'event_without_transaction_id',
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
            'event' => univapayWebhookEventName($payload) ?: null,
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
}
if (!defined('UNIVAPAY_WEBHOOK_DISABLE_AUTO_RUN')) {
    univapayHandleWebhookRequest();
}
