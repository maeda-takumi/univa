<?php

require_once __DIR__ . '/lib/univapay_transactions.php';

$dbPath = __DIR__ . '/univapay_transaction_history.sqlite';
$configPath = __DIR__ . '/config.php';
$config = univapayLoadConfig($configPath);
$univapayConfig = is_array($config['univapay'] ?? null) ? $config['univapay'] : [];
[$secret, $jwt] = univapayResolveCredentials($config);
$baseUrl = (string)($univapayConfig['api_base_url'] ?? UNIVAPAY_TRANSACTION_HISTORY_BASE_URL);
$mode = (string)($univapayConfig['mode'] ?? 'live');

$startDate = $_POST['start_date'] ?? gmdate('Y-m-01');
$endDate = $_POST['end_date'] ?? gmdate('Y-m-d');
$message = null;
$error = null;
$isSubmitted = $_SERVER['REQUEST_METHOD'] === 'POST';
$sheetImportedMessage = null;


function normalizeSheetValue($value): ?string
{
    if ($value === null) {
        return null;
    }
    $trimmed = trim((string)$value);
    return $trimmed === '' ? null : $trimmed;
}

function fetchSheetRows(array $sheetConfig): array
{
    $spreadsheetId = (string)($sheetConfig['spreadsheet_id'] ?? '');
    $sheetName = (string)($sheetConfig['sheet_name'] ?? '');
    if ($spreadsheetId === '' || $sheetName === '') {
        throw new RuntimeException('config.php のシート設定が不足しています。');
    }

    $serviceAccountPath = 'service_account.json';
    if (!file_exists($serviceAccountPath)) {
        throw new RuntimeException('service_account.json が見つかりません。');
    }
    $serviceAccount = json_decode((string)file_get_contents($serviceAccountPath), true);
    if (!is_array($serviceAccount) || empty($serviceAccount['client_email']) || empty($serviceAccount['private_key'])) {
        throw new RuntimeException('service_account.json の内容が不正です。');
    }

    $issuedAt = time();
    $expiresAt = $issuedAt + 3600;
    $jwtHeader = rtrim(strtr(base64_encode(json_encode(['alg' => 'RS256', 'typ' => 'JWT'])), '+/', '-_'), '=');
    $jwtPayload = rtrim(strtr(base64_encode(json_encode([
        'iss' => $serviceAccount['client_email'],
        'scope' => 'https://www.googleapis.com/auth/spreadsheets.readonly',
        'aud' => 'https://oauth2.googleapis.com/token',
        'iat' => $issuedAt,
        'exp' => $expiresAt,
    ])), '+/', '-_'), '=');
    $unsignedJwt = $jwtHeader . '.' . $jwtPayload;
    $signature = '';
    $signed = openssl_sign($unsignedJwt, $signature, $serviceAccount['private_key'], OPENSSL_ALGO_SHA256);
    if (!$signed) {
        throw new RuntimeException('サービスアカウント署名の生成に失敗しました。');
    }
    $assertion = $unsignedJwt . '.' . rtrim(strtr(base64_encode($signature), '+/', '-_'), '=');

    $tokenRequest = http_build_query([
        'grant_type' => 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion' => $assertion,
    ]);
    $tokenResponse = file_get_contents('https://oauth2.googleapis.com/token', false, stream_context_create([
        'http' => [
            'method' => 'POST',
            'header' => "Content-Type: application/x-www-form-urlencoded\r\n",
            'content' => $tokenRequest,
            'timeout' => 30,
        ],
    ]));
    if ($tokenResponse === false) {
        throw new RuntimeException('Google OAuthトークンの取得に失敗しました。');
    }
    $tokenDecoded = json_decode($tokenResponse, true);
    $accessToken = $tokenDecoded['access_token'] ?? null;
    if (!is_string($accessToken) || $accessToken === '') {
        throw new RuntimeException('Google OAuthトークンのレスポンスが不正です。');
    }

    $range = rawurlencode($sheetName . '!A:AB');
    $url = sprintf(
        'https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s?majorDimension=ROWS',
        rawurlencode($spreadsheetId),
        $range
    );

    $response = file_get_contents($url, false, stream_context_create([
        'http' => [
            'method' => 'GET',
            'header' => "Authorization: Bearer {$accessToken}\r\n",
            'timeout' => 30,
        ],
    ]));
    if ($response === false) {
        throw new RuntimeException('Google Sheets API の取得に失敗しました。');
    }

    $decoded = json_decode($response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Google Sheets API のレスポンス解析に失敗しました。');
    }

    return $decoded['values'] ?? [];
}

if ($isSubmitted && (($_POST['action'] ?? '') === 'import_sheet')) {
    try {
        if (!file_exists($configPath)) {
            throw new RuntimeException('config.php が見つかりません。');
        }
        $config = require $configPath;
        $sheetConfig = $config['google_sheet'] ?? [];
        $rows = fetchSheetRows($sheetConfig);

        $pdo = new PDO('sqlite:' . $dbPath);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $requiredColumns = [
            'serial_number','sales_year_month','payment_year_month','real_name','system_name','entry_point','state','line_name','phone_number','email','sales_date','payment_date','expected_payment_amount','payment_amount','payment_count','login_id','payment_destination','video_person_in_charge','sales_person_in_charge','acquisition_channel','age','system_delivery_status','remarks','payment_week','data1','data2','line_registration_date','imported_at'
        ];
        $existingColumns = [];
        $tableInfo = $pdo->query('PRAGMA table_info(spreadsheet_imports)');
        if ($tableInfo !== false) {
            while ($column = $tableInfo->fetch(PDO::FETCH_ASSOC)) {
                $name = (string)($column['name'] ?? '');
                if ($name !== '') {
                    $existingColumns[$name] = true;
                }
            }
        }
        if (!empty($existingColumns)) {
            foreach ($requiredColumns as $columnName) {
                if (!isset($existingColumns[$columnName])) {
                    $pdo->exec('ALTER TABLE spreadsheet_imports ADD COLUMN ' . $columnName . ' TEXT');
                }
            }
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS spreadsheet_imports (
                id TEXT PRIMARY KEY,
                serial_number TEXT,
                sales_year_month TEXT,
                payment_year_month TEXT,
                real_name TEXT,
                system_name TEXT,
                entry_point TEXT,
                state TEXT,
                line_name TEXT,
                phone_number TEXT,
                email TEXT,
                sales_date TEXT,
                payment_date TEXT,
                expected_payment_amount TEXT,
                payment_amount TEXT,
                payment_count TEXT,
                login_id TEXT,
                payment_destination TEXT,
                video_person_in_charge TEXT,
                sales_person_in_charge TEXT,
                acquisition_channel TEXT,
                age TEXT,
                system_delivery_status TEXT,
                remarks TEXT,
                payment_week TEXT,
                data1 TEXT,
                data2 TEXT,
                line_registration_date TEXT,
                imported_at TEXT
            )'
        );

        $stmt = $pdo->prepare(
            'INSERT INTO spreadsheet_imports (
                id, serial_number, sales_year_month, payment_year_month, real_name, system_name, entry_point, state, line_name, phone_number, email, sales_date, payment_date, expected_payment_amount, payment_amount, payment_count, login_id, payment_destination, video_person_in_charge, sales_person_in_charge, acquisition_channel, age, system_delivery_status, remarks, payment_week, data1, data2, line_registration_date, imported_at
            ) VALUES (
                :id, :serial_number, :sales_year_month, :payment_year_month, :real_name, :system_name, :entry_point, :state, :line_name, :phone_number, :email, :sales_date, :payment_date, :expected_payment_amount, :payment_amount, :payment_count, :login_id, :payment_destination, :video_person_in_charge, :sales_person_in_charge, :acquisition_channel, :age, :system_delivery_status, :remarks, :payment_week, :data1, :data2, :line_registration_date, :imported_at
            ) ON CONFLICT(id) DO UPDATE SET
                serial_number=excluded.serial_number, sales_year_month=excluded.sales_year_month, payment_year_month=excluded.payment_year_month, real_name=excluded.real_name, system_name=excluded.system_name, entry_point=excluded.entry_point, state=excluded.state, line_name=excluded.line_name, phone_number=excluded.phone_number, email=excluded.email, sales_date=excluded.sales_date, payment_date=excluded.payment_date, expected_payment_amount=excluded.expected_payment_amount, payment_amount=excluded.payment_amount, payment_count=excluded.payment_count, login_id=excluded.login_id, payment_destination=excluded.payment_destination, video_person_in_charge=excluded.video_person_in_charge, sales_person_in_charge=excluded.sales_person_in_charge, acquisition_channel=excluded.acquisition_channel, age=excluded.age, system_delivery_status=excluded.system_delivery_status, remarks=excluded.remarks, payment_week=excluded.payment_week, data1=excluded.data1, data2=excluded.data2, line_registration_date=excluded.line_registration_date, imported_at=excluded.imported_at'
        );

        $imported = 0;
        foreach ($rows as $row) {
            $id = normalizeSheetValue($row[0] ?? null);
            if ($id === null) {
                continue;
            }
            $stmt->execute([
                ':id' => $id, ':serial_number' => normalizeSheetValue($row[1] ?? null), ':sales_year_month' => normalizeSheetValue($row[2] ?? null), ':payment_year_month' => normalizeSheetValue($row[3] ?? null), ':real_name' => normalizeSheetValue($row[4] ?? null), ':system_name' => normalizeSheetValue($row[5] ?? null), ':entry_point' => normalizeSheetValue($row[6] ?? null), ':state' => normalizeSheetValue($row[7] ?? null), ':line_name' => normalizeSheetValue($row[8] ?? null), ':phone_number' => normalizeSheetValue($row[9] ?? null), ':email' => normalizeSheetValue($row[10] ?? null), ':sales_date' => normalizeSheetValue($row[11] ?? null), ':payment_date' => normalizeSheetValue($row[12] ?? null), ':expected_payment_amount' => normalizeSheetValue($row[13] ?? null), ':payment_amount' => normalizeSheetValue($row[14] ?? null), ':payment_count' => normalizeSheetValue($row[15] ?? null), ':login_id' => normalizeSheetValue($row[16] ?? null), ':payment_destination' => normalizeSheetValue($row[17] ?? null), ':video_person_in_charge' => normalizeSheetValue($row[18] ?? null), ':sales_person_in_charge' => normalizeSheetValue($row[19] ?? null), ':acquisition_channel' => normalizeSheetValue($row[20] ?? null), ':age' => normalizeSheetValue($row[21] ?? null), ':system_delivery_status' => normalizeSheetValue($row[22] ?? null), ':remarks' => normalizeSheetValue($row[23] ?? null), ':payment_week' => normalizeSheetValue($row[24] ?? null), ':data1' => normalizeSheetValue($row[25] ?? null), ':data2' => normalizeSheetValue($row[26] ?? null), ':line_registration_date' => normalizeSheetValue($row[27] ?? null), ':imported_at' => gmdate('c'),
            ]);
            $imported++;
        }
        $sheetImportedMessage = "スプレッドシート取込完了: {$imported}件";
    } catch (Throwable $e) {
        $error = 'スプレッドシート取込エラー: ' . $e->getMessage();
    }
} elseif ($isSubmitted) {
    try {
        $result = univapayFetchAndStore($startDate, $endDate, [
            'secret' => $secret,
            'jwt' => $jwt,
            'base_url' => $baseUrl,
            'mode' => $mode,
            'db_path' => $dbPath,
        ]);

        $message = sprintf(
            'DB保存完了: %d件 / 取得件数: %d件 / 保存先: %s',
            $result['saved_count'],
            $result['fetched_count'],
            $result['db_path']
        );
    } catch (Throwable $e) {
        $error = 'UniVaPay API取得エラー: ' . $e->getMessage();
    }
}

?>
<!doctype html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>UnivaPay 取引履歴取得</title>
    <style>
        body { font-family: "Hiragino Kaku Gothic ProN", "Yu Gothic", sans-serif; background:#f5f7fb; color:#1f2937; margin:0; padding:24px; }
        .container { max-width:760px; margin:0 auto; background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:24px; box-shadow:0 8px 24px rgba(15,23,42,.06); }
        h1 { margin-top:0; font-size:24px; }
        .description { color:#4b5563; margin-bottom:18px; }
        form { display:grid; gap:14px; }
        label { display:grid; gap:6px; font-weight:600; }
        input[type="date"] { padding:10px 12px; border:1px solid #cbd5e1; border-radius:8px; font-size:14px; }
        button { background:#2563eb; color:#fff; border:none; border-radius:8px; padding:11px 18px; font-weight:700; width:140px; cursor:pointer; }
        button:disabled { background:#93c5fd; cursor:wait; }
        .status { margin-top:16px; padding:12px 14px; border-radius:8px; display:none; font-weight:600; }
        .status.show { display:block; }
        .status.running { background:#eff6ff; border:1px solid #93c5fd; color:#1d4ed8; }
        .status.success { background:#ecfdf5; border:1px solid #86efac; color:#166534; }
        .status.error { background:#fef2f2; border:1px solid #fca5a5; color:#b91c1c; }
    </style>
</head>
<body>
    <div class="container">
    <h1>データ取込</h1>
    <p class="description">API取得とスプレッドシート取得を区分けしています。必要な方だけ実行してください。</p>

    <h2>1. UniVaPay API 取得（期間指定）</h2>

    <form method="post" id="fetchForm">
        <input type="hidden" name="action" value="fetch_univapay">
        <label>
            取得期間(開始)
            <input type="date" name="start_date" value="<?= htmlspecialchars($startDate, ENT_QUOTES, 'UTF-8') ?>" required>
        </label>
        <br>
        <label>
            取得期間(終了)
            <input type="date" name="end_date" value="<?= htmlspecialchars($endDate, ENT_QUOTES, 'UTF-8') ?>" required>
        </label>
        <button type="submit" id="submitButton">実行</button>
    </form>
    <h2 style="margin-top:20px;">2. スプレッドシート取込（A列〜AB列 / 全行）</h2>
    <form method="post" style="margin-top: 12px;" id="sheetImportForm">
        <input type="hidden" name="action" value="import_sheet">
        <button type="submit" id="sheetImportButton">スプレッドシート取込</button>
    </form>

    <div id="runningStatus" class="status" aria-live="polite"></div>

    <?php if ($message): ?>
        <p class="status success show"><?= htmlspecialchars($message, ENT_QUOTES, 'UTF-8') ?></p>
    <?php endif; ?>

    <?php if ($error): ?>
        <p class="status error show"><?= htmlspecialchars($error, ENT_QUOTES, 'UTF-8') ?></p>
    <?php endif; ?>
    <?php if ($sheetImportedMessage): ?>
        <p class="status success show"><?= htmlspecialchars($sheetImportedMessage, ENT_QUOTES, 'UTF-8') ?></p>
    <?php endif; ?>
    </div>

    <script>
        const fetchForm = document.getElementById('fetchForm');
        const submitButton = document.getElementById('submitButton');
        const sheetImportForm = document.getElementById('sheetImportForm');
        const sheetImportButton = document.getElementById('sheetImportButton');
        const runningStatus = document.getElementById('runningStatus');

        if (fetchForm) {
            fetchForm.addEventListener('submit', () => {
                runningStatus.textContent = '実行中です... API取得とDB保存が完了するまでお待ちください。';
                runningStatus.className = 'status running show';
                submitButton.disabled = true;
                submitButton.textContent = '実行中...';
            });
        }

        if (sheetImportForm) {
            sheetImportForm.addEventListener('submit', () => {
                runningStatus.textContent = '実行中です... スプレッドシート取込が完了するまでお待ちください。';
                runningStatus.className = 'status running show';
                sheetImportButton.disabled = true;
                sheetImportButton.textContent = '取込中...';
            });
        }
    </script>
</body>
</html>
