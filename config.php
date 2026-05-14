<?php
return [
    'google_sheet' => [
        'spreadsheet_id' => '1BHca4zzFQfwbJ4bzyuvPpIn8gXN7aF51Gb_tz3RmEZ4',
        'sheet_name' => 'ALL投資顧客管理',
    ],
    'univapay' => [
        'secret' => getenv('UNIVAPAY_SECRET') ?: '9X9sL29YItHjZgYvK553',
        'jwt' => getenv('UNIVAPAY_JWT') ?: 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhcHBfdG9rZW4iLCJpYXQiOjE3Nzc2MTE5ODAsIm1lcmNoYW50X2lkIjoiMTFmMDgzNWYtMjBmYi1kNWEyLWE0NzEtZGY2YzExOTgwNmNkIiwic3RvcmVfaWQiOiIxMWYwODM1Zi0yMjc4LWFkMDYtOWFlYy01M2I1ZjYxZWFjZTYiLCJkb21haW5zIjpbXSwibW9kZSI6ImxpdmUiLCJjcmVhdG9yX2lkIjoiMTFmMDgzNWYtMjBmYi1kNWEyLWE0NzEtZGY2YzExOTgwNmNkIiwidmVyc2lvbiI6MSwianRpIjoiMTFmMTQ1MWItN2RiZC1jZWFjLWIxZDAtOTdhNjc4NGEwNTVhIn0.0q25f34nwO2YFI7Jqkr6SAkMbQodke_NIBX41G8oXFM',
        'api_base_url' => 'https://api.univapay.com/transaction_history',
        'mode' => 'live',
        // Webhook受信日を含めて何日分を再取得するか。1なら受信日だけをAPI取得します。
        'webhook_fetch_days' => 1,
        // 設定すると Webhook/API実行用エンドポイントで共有シークレット検証を行います。
        'webhook_secret' => getenv('UNIVAPAY_WEBHOOK_SECRET') ?: '',
        'fetch_endpoint_secret' => getenv('UNIVAPAY_FETCH_ENDPOINT_SECRET') ?: '',
    ],
];