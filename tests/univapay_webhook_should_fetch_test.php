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

echo "All webhook should-fetch tests passed.\n";
