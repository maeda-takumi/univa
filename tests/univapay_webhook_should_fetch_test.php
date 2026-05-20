<?php

define('UNIVAPAY_WEBHOOK_DISABLE_AUTO_RUN', true);
require_once __DIR__ . '/../univapay_webhook.php';

$cases = [
    'token_created webhook skips transaction fetch' => [
        [
            'event' => 'token_created',
            'data' => [
                'id' => '11f14f56-d9b7-11ec-89a3-bf2017b5f1e2',
                'type' => 'recurring',
                'payment_type' => 'bank_transfer',
            ],
        ],
        false,
        'token_event_without_transaction_history',
    ],
    'token_three_ds_updated webhook skips transaction fetch' => [
        [
            'id' => '11f14f61-35d3-be6c-9741-016d40fb6880',
            'event' => 'token_three_ds_updated',
            'data' => [
                'id' => '11f14f4a-a082-155e-8aa5-fb98d40da2d0',
                'type' => 'recurring',
                'payment_type' => 'card',
                'data' => [
                    'three_ds' => [
                        'enabled' => true,
                        'status' => 'successful',
                    ],
                ],
            ],
        ],
        false,
        'token_event_without_transaction_history',
    ],
    'json-string encoded token webhook skips transaction fetch after nested decode' => [
        univapayWebhookDecodePayload(json_encode(json_encode([
            'event' => 'token_three_ds_updated',
            'data' => [
                'id' => '11f14f4a-a082-155e-8aa5-fb98d40da2d0',
                'type' => 'recurring',
                'payment_type' => 'card',
            ],
        ], JSON_UNESCAPED_UNICODE), JSON_UNESCAPED_UNICODE)),
        false,
        'token_event_without_transaction_history',
    ],
    'token-shaped payload without event skips transaction fetch' => [
        [
            'data' => [
                'id' => '11f14f56-d9b7-11ec-89a3-bf2017b5f1e2',
                'type' => 'recurring',
                'payment_type' => 'bank_transfer',
            ],
        ],
        false,
        'payment_token_without_transaction_history',
    ],
    'charge webhook fetches transaction history' => [
        [
            'event' => 'charge_finished',
            'data' => [
                'charge_id' => 'charge-123',
            ],
        ],
        true,
        null,
    ],
    'transaction webhook fetches transaction history' => [
        [
            'event' => 'transaction_updated',
            'data' => [
                'transaction_id' => 'txn-123',
            ],
        ],
        true,
        null,
    ],
    'charge webhook with charge id in data id fetches transaction history' => [
        [
            'id' => 'webhook-event-123',
            'event' => 'charge_finished',
            'data' => [
                'id' => 'charge-123',
            ],
        ],
        true,
        null,
    ],
    'chargeback webhook with resource id fetches transaction history' => [
        [
            'event' => 'chargeback_created',
            'data' => [
                'resource_id' => 'txn-chargeback-123',
            ],
        ],
        true,
        null,
    ],
];

foreach ($cases as $label => [$payload, $expectedShouldFetch, $expectedSkipReason]) {
    $actualShouldFetch = univapayWebhookShouldFetchTransactions($payload);
    if ($actualShouldFetch !== $expectedShouldFetch) {
        fwrite(STDERR, sprintf(
            "%s: expected shouldFetch=%s, got %s\n",
            $label,
            var_export($expectedShouldFetch, true),
            var_export($actualShouldFetch, true)
        ));
        exit(1);
    }

    $actualSkipReason = univapayWebhookSkipFetchReason($payload);
    if ($actualSkipReason !== $expectedSkipReason) {
        fwrite(STDERR, sprintf(
            "%s: expected skipReason=%s, got %s\n",
            $label,
            var_export($expectedSkipReason, true),
            var_export($actualSkipReason, true)
        ));
        exit(1);
    }
}

$chargebackPayload = [
    'event' => 'chargeback_created',
    'data' => [
        'charge_id' => 'charge-123',
    ],
];
if (univapayDetectWebhookStatus($chargebackPayload) !== 'chargeback') {
    fwrite(STDERR, "chargeback webhook should be detected as chargeback\n");
    exit(1);
}

if (univapayResultKindFromItem(['type' => 'refund', 'payment_type' => 'card']) !== 'refund') {
    fwrite(STDERR, "refund transaction should be classified as refund result kind\n");
    exit(1);
}

if (univapayResultKindFromItem(['type' => 'charge', 'payment_type' => 'bank_transfer']) !== 'transfer') {
    fwrite(STDERR, "bank transfer charge should be classified as transfer result kind\n");
    exit(1);
}

[$resourceId, $chargeId] = univapayWebhookReferenceIds([
    'id' => 'webhook-event-123',
    'event' => 'charge_finished',
    'data' => [
        'id' => 'charge-123',
    ],
]);
if ($resourceId !== '' || $chargeId !== 'charge-123') {
    fwrite(STDERR, "webhook reference ids should treat charge data.id as charge id\n");
    exit(1);
}

$eventDate = univapayWebhookEventDate(
    ['data' => ['created_on' => '2026-04-15T12:34:56Z']],
    new DateTimeImmutable('2026-05-20T00:00:00Z')
);
if ($eventDate->format('Y-m-d') !== '2026-04-15') {
    fwrite(STDERR, "webhook event date should prefer payload created_on\n");
    exit(1);
}

echo "All webhook should-fetch tests passed.\n";
