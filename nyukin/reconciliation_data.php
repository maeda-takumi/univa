<?php
declare(strict_types=1);

function u(string $escaped): string
{
    return json_decode('"' . $escaped . '"', false, 512, JSON_THROW_ON_ERROR);
}

function csv_path(string $pattern): string
{
    $files = glob(__DIR__ . DIRECTORY_SEPARATOR . $pattern);
    if (!$files) {
        return '';
    }
    usort($files, fn($a, $b) => filemtime($b) <=> filemtime($a));
    return $files[0];
}

function read_csv_assoc(string $path): array
{
    if ($path === '' || !is_readable($path)) return [];
    $handle = fopen($path, 'rb');
    if (!$handle) return [];
    $header = fgetcsv($handle);
    if (!$header) {
        fclose($handle);
        return [];
    }
    $header[0] = preg_replace('/^\xEF\xBB\xBF/', '', (string)$header[0]);
    $rows = [];
    while (($row = fgetcsv($handle)) !== false) {
        $item = [];
        foreach ($header as $index => $name) {
            $item[(string)$name] = $row[$index] ?? '';
        }
        $rows[] = $item;
    }
    fclose($handle);
    return $rows;
}

function col(string $name): string
{
    static $cols = null;
    if ($cols === null) {
        $cols = [
            'id' => 'ID',
            'name' => u('\u672c\u540d'),
            'system' => u('\u30b7\u30b9\u30c6\u30e0\u540d'),
            'status' => u('\u72b6\u614b'),
            'email' => u('\u30e1\u30fc\u30eb\u30a2\u30c9\u30ec\u30b9'),
            'db_date' => u('\u5165\u91d1\u65e5'),
            'planned' => u('\u5165\u91d1\u4e88\u5b9a\u984d'),
            'db_amount' => u('\u5165\u91d1\u984d'),
            'nth' => u('\u652f\u6255\u3044\u4f55\u56de\u76ee'),
            'dest' => u('\u5165\u91d1\u5148'),
            'sales' => u('\u30bb\u30fc\u30eb\u30b9\u62c5\u5f53'),
            'tx_date' => u('\u65e5\u4ed8'),
            'tx_status' => u('\u72b6\u614b'),
            'method' => u('\u5165\u91d1\u65b9\u6cd5'),
            'tx_amount' => u('\u5165\u91d1\u984d'),
            'tx_name' => u('\u6c0f\u540d'),
        ];
    }
    return $cols[$name];
}

function dest_card(): string { return u('\u3010\u6c7a\u6e08\u3011\u30c8\u30e9\u30b9\u30c8Univa'); }
function dest_bank(): string { return u('\u3010\u53ce\u7d0d\u3011\u30c8\u30e9\u30b9\u30c8Univa'); }
function success_text(): string { return u('\u6210\u529f'); }

function normalize_amount(?string $value): int
{
    $value = str_replace([',', u('\uffe5'), u('\u00a5'), u('\u5186')], '', trim((string)$value));
    return $value === '' ? 0 : (int)round((float)$value);
}

function normalize_email(?string $value): string
{
    return strtolower(trim((string)$value));
}

function normalize_name(?string $value): string
{
    $value = function_exists('mb_strtolower') ? mb_strtolower(trim((string)$value), 'UTF-8') : strtolower(trim((string)$value));
    $value = str_replace(u('\u3000'), ' ', $value);
    return preg_replace('/\s+/u', '', $value) ?? '';
}

function normalize_date(?string $value, bool $has_time = false): string
{
    $value = trim((string)$value);
    if ($value === '') return '';
    if ($has_time) $value = explode(' ', $value)[0] ?? $value;
    $value = str_replace('/', '-', $value);
    $date = DateTime::createFromFormat('Y-m-d', $value);
    return $date ? $date->format('Y-m-d') : '';
}

function expected_dest(string $method): string
{
    return match ($method) {
        u('\u30ab\u30fc\u30c9') => dest_card(),
        u('\u632f\u8fbc') => dest_bank(),
        default => '',
    };
}

function db_entries(): array
{
    $entries = [];
    $customerPath = __DIR__ . DIRECTORY_SEPARATOR . 'data' . DIRECTORY_SEPARATOR . 'customer_payments.csv';
    $sourcePath = is_readable($customerPath) ? $customerPath : csv_path('ALL*.csv');
    foreach (read_csv_assoc($sourcePath) as $i => $row) {
        $date = normalize_date($row[col('db_date')] ?? '');
        $amount = normalize_amount($row[col('db_amount')] ?? '');
        $dest = trim((string)($row[col('dest')] ?? ''));
        if ($date === '' || $amount === 0 || !str_contains($dest, 'Univa')) continue;
        $line = $i + 2;
        $name = trim((string)($row[col('name')] ?? ''));
        $entries[] = [
            'key' => 'db-' . $line,
            'line' => $line,
            'id' => trim((string)($row[col('id')] ?? '')),
            'date' => $date,
            'name' => $name,
            'nameKey' => normalize_name($name),
            'email' => normalize_email($row[col('email')] ?? ''),
            'amount' => $amount,
            'dest' => $dest,
            'status' => trim((string)($row[col('status')] ?? '')),
            'system' => trim((string)($row[col('system')] ?? '')),
            'planned' => trim((string)($row[col('planned')] ?? '')),
            'nth' => trim((string)($row[col('nth')] ?? '')),
            'sales' => trim((string)($row[col('sales')] ?? '')),
        ];
    }
    return $entries;
}

function tx_entries(): array
{
    $entries = [];
    $rootDir = dirname(__DIR__);
    $dbFiles = glob($rootDir . DIRECTORY_SEPARATOR . '*.sqlite') ?: [];
    sort($dbFiles);
    $line = 1;

    foreach ($dbFiles as $dbFile) {
        try {
            $pdo = new PDO('sqlite:' . $dbFile, null, null, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]);

            $table = null;
            foreach (['transactions', 'transaction_history'] as $candidate) {
                $check = $pdo->query("SELECT name FROM sqlite_master WHERE type='table' AND name='{$candidate}'");
                if ($check !== false && $check->fetch()) {
                    $table = $candidate;
                    break;
                }
            }
            if ($table === null) continue;

            $stmt = $pdo->query(
                'SELECT created_on, status, payment_type, amount, metadata_name, cardholder_name, cardholder_email FROM ' . $table . ' ORDER BY created_on DESC'
            );
            while ($row = $stmt->fetch()) {
                if (trim((string)($row['status'] ?? '')) !== 'successful') continue;

                $datetime = format_tx_datetime((string)($row['created_on'] ?? ''));
                $date = normalize_date($datetime, true);
                $amount = normalize_amount((string)($row['amount'] ?? ''));
                if ($date === '' || $amount === 0) continue;

                $method = payment_method_label((string)($row['payment_type'] ?? ''));
                $name = trim((string)($row['metadata_name'] ?? ''));
                if ($name === '') {
                    $name = trim((string)($row['cardholder_name'] ?? ''));
                }

                $entries[] = [
                    'key' => 'tx-' . $line,
                    'line' => $line,
                    'datetime' => $datetime,
                    'date' => $date,
                    'name' => $name,
                    'nameKey' => normalize_name($name),
                    'email' => normalize_email($row['cardholder_email'] ?? ''),
                    'amount' => $amount,
                    'method' => $method,
                    'expectedDest' => expected_dest($method),
                ];
                $line++;
            }
        } catch (Throwable $e) {
            continue;
        }
    }

    if ($entries !== []) {
        return $entries;
    }

    foreach (read_csv_assoc(csv_path('transaction_history*.csv')) as $i => $row) {
        if (trim((string)($row[col('tx_status')] ?? '')) !== success_text()) continue;
        $date = normalize_date($row[col('tx_date')] ?? '', true);
        $amount = normalize_amount($row[col('tx_amount')] ?? '');
        if ($date === '' || $amount === 0) continue;
        $line = $i + 2;
        $method = trim((string)($row[col('method')] ?? ''));
        $name = trim((string)($row[col('tx_name')] ?? ''));
        $entries[] = [
            'key' => 'tx-' . $line,
            'line' => $line,
            'datetime' => trim((string)($row[col('tx_date')] ?? '')),
            'date' => $date,
            'name' => $name,
            'nameKey' => normalize_name($name),
            'email' => normalize_email($row[col('email')] ?? ''),
            'amount' => $amount,
            'method' => $method,
            'expectedDest' => expected_dest($method),
        ];
    }
    return $entries;
}

function format_tx_datetime(string $value): string
{
    $value = trim($value);
    if ($value === '') return '';

    try {
        $date = new DateTimeImmutable($value);
        $date = $date->setTimezone(new DateTimeZone('Asia/Tokyo'));
        return $date->format('Y-m-d H:i:s');
    } catch (Throwable $e) {
        return str_replace('T', ' ', preg_replace('/\.\d+Z$/', '', $value) ?? $value);
    }
}

function payment_method_label(string $paymentType): string
{
    return match ($paymentType) {
        'card' => u('\u30ab\u30fc\u30c9'),
        'bank_transfer' => u('\u632f\u8fbc'),
        default => $paymentType,
    };
}

function build_daily_data(): array
{
    $dbEntries = db_entries();
    $txEntries = tx_entries();
    $daily = [];
    foreach ($dbEntries as $row) {
        $daily[$row['date']] ??= ['date' => $row['date'], 'dbTotal' => 0, 'dbCount' => 0, 'txTotal' => 0, 'txCount' => 0];
        $daily[$row['date']]['dbTotal'] += $row['amount'];
        $daily[$row['date']]['dbCount']++;
    }
    foreach ($txEntries as $row) {
        $daily[$row['date']] ??= ['date' => $row['date'], 'dbTotal' => 0, 'dbCount' => 0, 'txTotal' => 0, 'txCount' => 0];
        $daily[$row['date']]['txTotal'] += $row['amount'];
        $daily[$row['date']]['txCount']++;
    }
    ksort($daily);
    $daily = array_values(array_map(function ($row) {
        $row['diff'] = $row['txTotal'] - $row['dbTotal'];
        return $row;
    }, $daily));
    return ['generatedAt' => date('Y-m-d H:i'), 'daily' => $daily, 'dbEntries' => $dbEntries, 'txEntries' => $txEntries];
}

function build_mistake_data(): array
{
    $dbRows = db_entries();
    $txRows = tx_entries();
    $usedTx = [];
    $usedDb = [];
    foreach ($txRows as $tx) {
        foreach ($dbRows as $db) {
            if (isset($usedDb[$db['key']])) continue;
            if ($tx['date'] === $db['date'] && $tx['nameKey'] === $db['nameKey'] && $tx['amount'] === $db['amount'] && $tx['expectedDest'] === $db['dest']) {
                $usedTx[$tx['key']] = true;
                $usedDb[$db['key']] = true;
                break;
            }
        }
    }
    $floatingTx = array_values(array_filter($txRows, fn($r) => !isset($usedTx[$r['key']])));
    $floatingDb = array_values(array_filter($dbRows, fn($r) => !isset($usedDb[$r['key']])));
    $suggestions = [];
    $matchedTx = [];
    $matchedDb = [];
    foreach ($floatingTx as $tx) {
        $best = candidate_for_tx($tx, $floatingDb);
        if ($best) {
            $suggestions[] = $best;
            $matchedTx[$tx['key']] = true;
            if ($best['db']) $matchedDb[$best['db']['key']] = true;
        }
    }
    foreach ($floatingTx as $tx) {
        if (!isset($matchedTx[$tx['key']])) {
            $suggestions[] = ['score' => 0, 'confidence' => u('\u672a\u5165\u529b'), 'kind' => u('\u81ea\u793eDB\u672a\u5165\u529b\u5019\u88dc'), 'fix' => "自社DBに {$tx['date']} / {$tx['name']} / " . number_format($tx['amount']) . "円 / {$tx['expectedDest']} を追加候補", 'why' => u('\u8fd1\u3044\u81ea\u793eDB\u30c7\u30fc\u30bf\u306a\u3057'), 'tx' => $tx, 'db' => null];
        }
    }
    foreach ($floatingDb as $db) {
        if (!isset($matchedDb[$db['key']])) {
            $suggestions[] = ['score' => 0, 'confidence' => u('\u4f59\u308a'), 'kind' => u('\u81ea\u793eDB\u306e\u307f\u5019\u88dc'), 'fix' => "自社DB 行{$db['line']} がUnivaPay成功データに対応するか確認", 'why' => u('\u8fd1\u3044UnivaPay\u30c7\u30fc\u30bf\u306a\u3057\u3001\u307e\u305f\u306f\u5225\u5019\u88dc\u306b\u7d10\u3065\u304d\u6e08\u307f'), 'tx' => null, 'db' => $db];
        }
    }
    $order = [u('\u5165\u91d1\u65e5\u5165\u529b\u30df\u30b9\u5019\u88dc') => 0, u('\u5165\u91d1\u984d\u5165\u529b\u30df\u30b9\u5019\u88dc') => 1, u('\u5165\u91d1\u5148\u5165\u529b\u30df\u30b9\u5019\u88dc') => 2, u('\u6c0f\u540d/\u9867\u5ba2\u7d10\u3065\u3051\u30df\u30b9\u5019\u88dc') => 3, u('\u8fd1\u3044\u30c7\u30fc\u30bf\u5019\u88dc') => 4, u('\u81ea\u793eDB\u672a\u5165\u529b\u5019\u88dc') => 5, u('\u81ea\u793eDB\u306e\u307f\u5019\u88dc') => 6];
    usort($suggestions, fn($a, $b) => [$order[$a['kind']] ?? 9, $a['tx']['date'] ?? $a['db']['date'], -$a['score']] <=> [$order[$b['kind']] ?? 9, $b['tx']['date'] ?? $b['db']['date'], -$b['score']]);
    $months = array_values(array_unique(array_filter(array_map(fn($r) => $r['tx'] ? substr($r['tx']['date'], 0, 7) : '', $suggestions))));
    rsort($months);
    return ['generatedAt' => date('Y-m-d H:i'), 'months' => $months, 'suggestions' => $suggestions, 'stats' => ['tx' => count($txRows), 'db' => count($dbRows), 'floatingTx' => count($floatingTx), 'floatingDb' => count($floatingDb), 'suggestions' => count($suggestions)]];
}

function candidate_for_tx(array $tx, array $floatingDb): ?array
{
    $out = [];
    foreach ($floatingDb as $db) {
        $score = 0;
        $reasons = [];
        if ($tx['email'] !== '' && $tx['email'] === $db['email']) { $score += 45; $reasons[] = u('\u30e1\u30fc\u30eb\u4e00\u81f4'); }
        if ($tx['nameKey'] !== '' && $tx['nameKey'] === $db['nameKey']) { $score += 35; $reasons[] = u('\u6c0f\u540d\u4e00\u81f4'); }
        if ($tx['amount'] === $db['amount']) { $score += 35; $reasons[] = u('\u91d1\u984d\u4e00\u81f4'); }
        if ($tx['date'] === $db['date']) { $score += 30; $reasons[] = u('\u5165\u91d1\u65e5\u4e00\u81f4'); }
        if ($tx['expectedDest'] === $db['dest']) { $score += 15; $reasons[] = u('\u5165\u91d1\u5148\u4e00\u81f4'); }
        if ($score < 65) continue;
        if ($tx['amount'] === $db['amount'] && $tx['date'] !== $db['date'] && ($tx['email'] === $db['email'] || $tx['nameKey'] === $db['nameKey'])) {
            $kind = u('\u5165\u91d1\u65e5\u5165\u529b\u30df\u30b9\u5019\u88dc'); $fix = "自社DB 行{$db['line']} の入金日を {$db['date']} から {$tx['date']} に変更候補"; $confidence = u('\u9ad8'); $score += 30;
        } elseif ($tx['date'] === $db['date'] && $tx['amount'] !== $db['amount'] && ($tx['email'] === $db['email'] || $tx['nameKey'] === $db['nameKey'])) {
            $kind = u('\u5165\u91d1\u984d\u5165\u529b\u30df\u30b9\u5019\u88dc'); $fix = "自社DB 行{$db['line']} の入金額を " . number_format($db['amount']) . "円 から " . number_format($tx['amount']) . "円 に変更候補"; $confidence = u('\u9ad8'); $score += 25;
        } elseif ($tx['date'] === $db['date'] && $tx['amount'] === $db['amount'] && $tx['nameKey'] !== $db['nameKey']) {
            $kind = u('\u6c0f\u540d/\u9867\u5ba2\u7d10\u3065\u3051\u30df\u30b9\u5019\u88dc'); $fix = "自社DB 行{$db['line']} が {$tx['name']} の入金として登録されていないか確認"; $confidence = u('\u4e2d'); $score += 15;
        } elseif ($tx['date'] === $db['date'] && $tx['amount'] === $db['amount'] && $tx['expectedDest'] !== $db['dest']) {
            $kind = u('\u5165\u91d1\u5148\u5165\u529b\u30df\u30b9\u5019\u88dc'); $fix = "自社DB 行{$db['line']} の入金先を {$tx['expectedDest']} に変更候補"; $confidence = u('\u9ad8'); $score += 20;
        } else {
            $kind = u('\u8fd1\u3044\u30c7\u30fc\u30bf\u5019\u88dc'); $fix = "UnivaPay 行{$tx['line']} と自社DB 行{$db['line']} が同一入金か確認"; $confidence = u('\u4e2d');
        }
        $out[] = ['score' => $score, 'confidence' => $confidence, 'kind' => $kind, 'fix' => $fix, 'why' => implode(' / ', $reasons), 'tx' => $tx, 'db' => $db];
    }
    usort($out, fn($a, $b) => $b['score'] <=> $a['score']);
    return $out[0] ?? null;
}

function app_json(string $type): string
{
    return json_encode(match ($type) { 'daily' => build_daily_data(), 'mistake' => build_mistake_data(), default => [] }, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
}
