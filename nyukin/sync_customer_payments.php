<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/lib/google_sheet_rows.php';

function syncSheetValue(array $row, int $index): ?string
{
    $value = trim((string)($row[$index] ?? ''));
    return $value === '' ? null : $value;
}

function syncPaymentRowsToSqlite(array $rows, string $dbPath): int
{
    if (count($rows) < 2) {
        return 0;
    }

    $pdo = new PDO('sqlite:' . $dbPath, null, null, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);

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
            id, serial_number, sales_year_month, payment_year_month, real_name, system_name, entry_point, state,
            line_name, phone_number, email, sales_date, payment_date, expected_payment_amount, payment_amount,
            payment_count, login_id, payment_destination, video_person_in_charge, sales_person_in_charge,
            acquisition_channel, age, system_delivery_status, remarks, payment_week, data1, data2,
            line_registration_date, imported_at
        ) VALUES (
            :id, :serial_number, :sales_year_month, :payment_year_month, :real_name, :system_name, :entry_point, :state,
            :line_name, :phone_number, :email, :sales_date, :payment_date, :expected_payment_amount, :payment_amount,
            :payment_count, :login_id, :payment_destination, :video_person_in_charge, :sales_person_in_charge,
            :acquisition_channel, :age, :system_delivery_status, :remarks, :payment_week, :data1, :data2,
            :line_registration_date, :imported_at
        ) ON CONFLICT(id) DO UPDATE SET
            serial_number=excluded.serial_number,
            sales_year_month=excluded.sales_year_month,
            payment_year_month=excluded.payment_year_month,
            real_name=excluded.real_name,
            system_name=excluded.system_name,
            entry_point=excluded.entry_point,
            state=excluded.state,
            line_name=excluded.line_name,
            phone_number=excluded.phone_number,
            email=excluded.email,
            sales_date=excluded.sales_date,
            payment_date=excluded.payment_date,
            expected_payment_amount=excluded.expected_payment_amount,
            payment_amount=excluded.payment_amount,
            payment_count=excluded.payment_count,
            login_id=excluded.login_id,
            payment_destination=excluded.payment_destination,
            video_person_in_charge=excluded.video_person_in_charge,
            sales_person_in_charge=excluded.sales_person_in_charge,
            acquisition_channel=excluded.acquisition_channel,
            age=excluded.age,
            system_delivery_status=excluded.system_delivery_status,
            remarks=excluded.remarks,
            payment_week=excluded.payment_week,
            data1=excluded.data1,
            data2=excluded.data2,
            line_registration_date=excluded.line_registration_date,
            imported_at=excluded.imported_at'
    );

    $imported = 0;
    $now = gmdate('c');
    foreach (array_slice($rows, 1) as $row) {
        if (!is_array($row)) {
            continue;
        }
        $id = syncSheetValue($row, 0);
        $paymentDate = syncSheetValue($row, 12);
        $paymentAmount = syncSheetValue($row, 14);
        $paymentDestination = syncSheetValue($row, 17);
        if ($id === null || ($paymentDate === null && $paymentAmount === null && $paymentDestination === null)) {
            continue;
        }

        $stmt->execute([
            ':id' => $id,
            ':serial_number' => syncSheetValue($row, 1),
            ':sales_year_month' => syncSheetValue($row, 2),
            ':payment_year_month' => syncSheetValue($row, 3),
            ':real_name' => syncSheetValue($row, 4),
            ':system_name' => syncSheetValue($row, 5),
            ':entry_point' => syncSheetValue($row, 6),
            ':state' => syncSheetValue($row, 7),
            ':line_name' => syncSheetValue($row, 8),
            ':phone_number' => syncSheetValue($row, 9),
            ':email' => syncSheetValue($row, 10),
            ':sales_date' => syncSheetValue($row, 11),
            ':payment_date' => $paymentDate,
            ':expected_payment_amount' => syncSheetValue($row, 13),
            ':payment_amount' => $paymentAmount,
            ':payment_count' => syncSheetValue($row, 15),
            ':login_id' => syncSheetValue($row, 16),
            ':payment_destination' => $paymentDestination,
            ':video_person_in_charge' => syncSheetValue($row, 18),
            ':sales_person_in_charge' => syncSheetValue($row, 19),
            ':acquisition_channel' => syncSheetValue($row, 20),
            ':age' => syncSheetValue($row, 21),
            ':system_delivery_status' => syncSheetValue($row, 22),
            ':remarks' => syncSheetValue($row, 23),
            ':payment_week' => syncSheetValue($row, 24),
            ':data1' => syncSheetValue($row, 25),
            ':data2' => syncSheetValue($row, 26),
            ':line_registration_date' => syncSheetValue($row, 27),
            ':imported_at' => $now,
        ]);
        $imported++;
    }

    return $imported;
}

function syncRedirectUrl(string $status, array $params = []): string
{
    $returnTo = (string)($_POST['return_to'] ?? $_SERVER['HTTP_REFERER'] ?? 'index.php');
    $isRootIndex = str_starts_with($returnTo, '../index.php');
    $parts = parse_url($returnTo);
    $path = (string)($parts['path'] ?? 'index.php');
    $base = basename($path);
    if (!in_array($base, ['index.php', 'payment_daily_dashboard.php', 'mistake_finder.php'], true)) {
        $base = 'index.php';
    }

    $query = [];
    if (!empty($parts['query'])) {
        parse_str((string)$parts['query'], $query);
    }
    unset($query['sync'], $query['count'], $query['message']);

    $query = array_merge($query, ['sync' => $status], $params);
    $target = $isRootIndex ? '../index.php' : $base;
    return $target . '?' . http_build_query($query);
}

if (($_SERVER['REQUEST_METHOD'] ?? '') !== 'POST') {
    header('Location: index.php');
    exit;
}

$dataDir = __DIR__ . '/data';
$csvPath = $dataDir . '/customer_payments.csv';
$metaPath = $dataDir . '/sync_meta.json';
$now = (new DateTimeImmutable('now', new DateTimeZone('Asia/Tokyo')))->format(DateTimeInterface::ATOM);

try {
    $config = googleSheetLoadConfig(dirname(__DIR__) . '/config.php');
    $rows = googleSheetFetchRows(
        is_array($config['google_sheet'] ?? null) ? $config['google_sheet'] : [],
        dirname(__DIR__) . '/service_account.json',
        'A:AK'
    );
    $count = googleSheetWriteCsv($rows, $csvPath);
    $paymentImportCount = syncPaymentRowsToSqlite($rows, dirname(__DIR__) . '/univapay_transaction_history.sqlite');
    googleSheetWriteSyncMeta($metaPath, [
        'status' => 'success',
        'synced_at' => $now,
        'row_count' => $count,
        'payment_import_count' => $paymentImportCount,
        'csv_path' => 'data/customer_payments.csv',
    ]);

    header('Location: ' . syncRedirectUrl('success', ['count' => (string)$count, 'payments' => (string)$paymentImportCount]));
    exit;
} catch (Throwable $e) {
    googleSheetWriteSyncMeta($metaPath, [
        'status' => 'error',
        'synced_at' => $now,
        'message' => $e->getMessage(),
        'csv_path' => 'data/customer_payments.csv',
    ]);

    header('Location: ' . syncRedirectUrl('error', ['message' => $e->getMessage()]));
    exit;
}
