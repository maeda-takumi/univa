<?php
declare(strict_types=1);

function googleSheetLoadConfig(string $configPath): array
{
    if (!is_file($configPath)) {
        throw new RuntimeException('config.php not found.');
    }

    $config = require $configPath;
    if (!is_array($config)) {
        throw new RuntimeException('config.php is invalid.');
    }

    return $config;
}

function googleSheetFetchRows(array $sheetConfig, string $serviceAccountPath, string $columns = 'A:AK'): array
{
    $spreadsheetId = (string)($sheetConfig['spreadsheet_id'] ?? '');
    $sheetName = (string)($sheetConfig['sheet_name'] ?? '');
    if ($spreadsheetId === '' || $sheetName === '') {
        throw new RuntimeException('Google sheet config is missing.');
    }

    if (!is_file($serviceAccountPath)) {
        throw new RuntimeException('service_account.json not found.');
    }

    $serviceAccount = json_decode((string)file_get_contents($serviceAccountPath), true);
    if (!is_array($serviceAccount) || empty($serviceAccount['client_email']) || empty($serviceAccount['private_key'])) {
        throw new RuntimeException('service_account.json is invalid.');
    }

    $issuedAt = time();
    $jwtHeader = googleSheetBase64UrlEncode(json_encode(['alg' => 'RS256', 'typ' => 'JWT'], JSON_THROW_ON_ERROR));
    $jwtPayload = googleSheetBase64UrlEncode(json_encode([
        'iss' => $serviceAccount['client_email'],
        'scope' => 'https://www.googleapis.com/auth/spreadsheets.readonly',
        'aud' => 'https://oauth2.googleapis.com/token',
        'iat' => $issuedAt,
        'exp' => $issuedAt + 3600,
    ], JSON_THROW_ON_ERROR));

    $unsignedJwt = $jwtHeader . '.' . $jwtPayload;
    $signature = '';
    if (!openssl_sign($unsignedJwt, $signature, $serviceAccount['private_key'], OPENSSL_ALGO_SHA256)) {
        throw new RuntimeException('Failed to sign service account JWT.');
    }

    $tokenResponse = googleSheetRequest('https://oauth2.googleapis.com/token', [
        'method' => 'POST',
        'header' => "Content-Type: application/x-www-form-urlencoded\r\n",
        'content' => http_build_query([
            'grant_type' => 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion' => $unsignedJwt . '.' . googleSheetBase64UrlEncode($signature),
        ]),
    ]);

    $tokenDecoded = json_decode($tokenResponse, true);
    $accessToken = $tokenDecoded['access_token'] ?? null;
    if (!is_string($accessToken) || $accessToken === '') {
        throw new RuntimeException('Google OAuth token response is invalid.');
    }

    $range = rawurlencode($sheetName . '!' . $columns);
    $url = sprintf(
        'https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s?majorDimension=ROWS',
        rawurlencode($spreadsheetId),
        $range
    );

    $response = googleSheetRequest($url, [
        'method' => 'GET',
        'header' => "Authorization: Bearer {$accessToken}\r\n",
    ]);

    $decoded = json_decode($response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Google Sheets API response is invalid.');
    }

    return is_array($decoded['values'] ?? null) ? $decoded['values'] : [];
}

function googleSheetWriteCsv(array $rows, string $csvPath): int
{
    $dir = dirname($csvPath);
    if (!is_dir($dir) && !mkdir($dir, 0775, true) && !is_dir($dir)) {
        throw new RuntimeException('Failed to create CSV directory.');
    }

    $tmpPath = $csvPath . '.tmp';
    $handle = fopen($tmpPath, 'wb');
    if ($handle === false) {
        throw new RuntimeException('Failed to create temporary CSV file.');
    }

    fwrite($handle, "\xEF\xBB\xBF");
    foreach ($rows as $row) {
        if (!is_array($row)) {
            continue;
        }
        fputcsv($handle, array_map(static fn($value): string => (string)$value, $row));
    }
    fclose($handle);

    if (!rename($tmpPath, $csvPath)) {
        @unlink($tmpPath);
        throw new RuntimeException('Failed to update CSV file.');
    }

    return max(count($rows) - 1, 0);
}

function googleSheetWriteSyncMeta(string $metaPath, array $meta): void
{
    $dir = dirname($metaPath);
    if (!is_dir($dir) && !mkdir($dir, 0775, true) && !is_dir($dir)) {
        throw new RuntimeException('Failed to create sync metadata directory.');
    }

    file_put_contents(
        $metaPath,
        json_encode($meta, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT)
    );
}

function googleSheetBase64UrlEncode(string $value): string
{
    return rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
}

function googleSheetRequest(string $url, array $httpOptions): string
{
    $httpOptions += ['timeout' => 30];
    $response = file_get_contents($url, false, stream_context_create(['http' => $httpOptions]));
    if ($response === false) {
        throw new RuntimeException('Google API request failed.');
    }

    return $response;
}
