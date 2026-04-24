<?php
declare(strict_types=1);

$pageTitle = '入金データ一覧';

const DB_FILE = __DIR__ . '/data/univapay_webhook.sqlite';
const PER_PAGE = 25;

function h(?string $value): string
{
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function get_nested_value(array $source, array $path): mixed
{
    $current = $source;
    foreach ($path as $key) {
        if (!is_array($current) || !array_key_exists($key, $current)) {
            return null;
        }
        $current = $current[$key];
    }
    return $current;
}

function first_non_empty_value(array $values): ?string
{
    foreach ($values as $value) {
        if ($value === null) {
            continue;
        }

        $value = trim((string)$value);
        if ($value !== '') {
            return $value;
        }
    }

    return null;
}

function parse_json_if_needed(mixed $value): mixed
{
    if (!is_string($value)) {
        return $value;
    }

    $value = trim($value);
    if ($value === '') {
        return $value;
    }

    $decoded = json_decode($value, true);
    if (json_last_error() === JSON_ERROR_NONE) {
        return $decoded;
    }

    return $value;
}

function format_display_datetime(?string $value): string
{
    if ($value === null) {
        return '';
    }

    $text = trim($value);
    if ($text === '') {
        return '';
    }

    $formats = [
        'Y/m/d H:i:s',
        'Y-m-d H:i:s',
        DateTimeInterface::ATOM,
    ];

    foreach ($formats as $format) {
        $date = DateTimeImmutable::createFromFormat($format, $text);
        if ($date instanceof DateTimeImmutable) {
            return $date->format('Y/m/d H:i:s');
        }
    }

    try {
        return (new DateTimeImmutable($text))->format('Y/m/d H:i:s');
    } catch (Throwable) {
        return $text;
    }
}

function extract_display_data(array $row): array
{
    $payload = json_decode((string)($row['raw_json'] ?? ''), true);
    if (!is_array($payload)) {
        $payload = [];
    }

    $tokenMetadata = parse_json_if_needed($payload['トークンメタデータ'] ?? null);
    if (!is_array($tokenMetadata)) {
        $tokenMetadata = [];
    }

    $chargeMetadata = parse_json_if_needed($payload['課金メタデータ'] ?? null);
    if (!is_array($chargeMetadata)) {
        $chargeMetadata = [];
    }

    $webhookMetadata = parse_json_if_needed(get_nested_value($payload, ['data', 'metadata']));
    if (!is_array($webhookMetadata)) {
        $webhookMetadata = [];
    }
    $paymentDate = first_non_empty_value([
        $row['payment_date'] ?? null,
        $payload['入金日'] ?? null,
        $payload['イベント作成日時'] ?? null,
        $payload['課金作成日時'] ?? null,
        get_nested_value($payload, ['data', 'created_on']),
        $row['received_at'] ?? null,
    ]);

    $paymentAmount = first_non_empty_value([
        $payload['入金額'] ?? null,
        $payload['課金金額'] ?? null,
        $payload['イベント金額'] ?? null,
        $payload['定期課金金額'] ?? null,
        get_nested_value($payload, ['data', 'charged_amount']),
        get_nested_value($payload, ['data', 'amount']),
    ]);

    $payerName = first_non_empty_value([
        $payload['入金者名'] ?? null,
        $payload['氏名'] ?? null,
        $payload['カード名義'] ?? null,
        $tokenMetadata['univapay-name'] ?? null,
        $tokenMetadata['name'] ?? null,
        $chargeMetadata['univapay-name'] ?? null,
        $chargeMetadata['name'] ?? null,
        $webhookMetadata['univapay-name'] ?? null,
        $webhookMetadata['name'] ?? null,
        get_nested_value($payload, ['customer', 'name']),
        get_nested_value($payload, ['data', 'customer_name']),
    ]);

    $email = first_non_empty_value([
        $payload['メールアドレス'] ?? null,
        get_nested_value($payload, ['customer', 'email']),
        get_nested_value($payload, ['data', 'email']),
        $tokenMetadata['email'] ?? null,
        $chargeMetadata['email'] ?? null,
        $webhookMetadata['email'] ?? null,
    ]);

    return [
        'payment_date' => format_display_datetime($paymentDate),
        'payment_amount' => $paymentAmount ?? '',
        'payer_name' => $payerName ?? '',
        'email' => $email ?? '',
        'event_type' => first_non_empty_value([
            $row['event_type'] ?? null,
            $payload['イベント'] ?? null,
            $payload['イベントタイプ'] ?? null,
            $payload['event_type'] ?? null,
            get_nested_value($payload, ['event', 'type']),
        ]) ?? '',
        'status' => trim((string)($row['status'] ?? '')),
    ];
}
function format_jpy_amount(mixed $amount): string
{
    if ($amount === null) {
        return '';
    }

    $raw = trim((string)$amount);
    if ($raw === '') {
        return '';
    }

    $normalized = str_replace([',', '¥', '円', ' '], '', $raw);
    if (!is_numeric($normalized)) {
        return $raw;
    }

    return '¥' . number_format((int)round((float)$normalized));
}

function status_badge_class(string $status): string
{
    $normalized = strtolower(trim($status));

    if ($normalized === '') {
        return 'status-neutral';
    }

    if (
        str_contains($normalized, 'success') ||
        str_contains($normalized, 'succeeded') ||
        str_contains($normalized, 'completed') ||
        str_contains($normalized, 'paid') ||
        str_contains($normalized, 'captured') ||
        str_contains($normalized, 'approved') ||
        str_contains($normalized, '完了') ||
        str_contains($normalized, '成功') ||
        str_contains($normalized, '入金済')
    ) {
        return 'status-success';
    }

    if (
        str_contains($normalized, 'pending') ||
        str_contains($normalized, 'processing') ||
        str_contains($normalized, 'in_progress') ||
        str_contains($normalized, 'authorized') ||
        str_contains($normalized, '保留') ||
        str_contains($normalized, '処理中')
    ) {
        return 'status-pending';
    }

    if (
        str_contains($normalized, 'refund') ||
        str_contains($normalized, 'chargeback') ||
        str_contains($normalized, 'reversed') ||
        str_contains($normalized, '返金') ||
        str_contains($normalized, '取消')
    ) {
        return 'status-warning';
    }

    if (
        str_contains($normalized, 'fail') ||
        str_contains($normalized, 'cancel') ||
        str_contains($normalized, 'error') ||
        str_contains($normalized, 'expired') ||
        str_contains($normalized, 'declined') ||
        str_contains($normalized, 'voided') ||
        str_contains($normalized, '失敗') ||
        str_contains($normalized, 'エラー') ||
        str_contains($normalized, '期限切れ') ||
        str_contains($normalized, 'キャンセル')
    ) {
        return 'status-danger';
    }

    return 'status-info';
}
function build_page_url(int $page, array $filters): string
{
    $params = ['page' => $page];
    foreach ($filters as $key => $value) {
        if ($value === '') {
            continue;
        }
        $params[$key] = $value;
    }
    return '?' . http_build_query($params);
}

$filters = [
    'payment_date_from' => trim((string)($_GET['payment_date_from'] ?? '')),
    'payment_date_to' => trim((string)($_GET['payment_date_to'] ?? '')),
    'payer_name' => trim((string)($_GET['payer_name'] ?? '')),
    'email' => trim((string)($_GET['email'] ?? '')),
    'event_type' => trim((string)($_GET['event_type'] ?? '売上')),
    'status' => trim((string)($_GET['status'] ?? '成功')),
];
$page = max(1, (int)($_GET['page'] ?? 1));

$dbExists = file_exists(DB_FILE);
$rows = [];
$totalCount = 0;
$totalPages = 1;
$errorMessage = '';
$infoMessage = '';
$summaryCount = 0;
$summaryAmount = 0;
$statusOptions = [];
$eventTypeOptions = [];

if ($dbExists) {
    try {
        $pdo = new PDO('sqlite:' . DB_FILE);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

        $tableInfoStmt = $pdo->query("PRAGMA table_info(webhook_events)");
        $tableColumns = array_map(
            static fn(array $row): string => (string)($row['name'] ?? ''),
            $tableInfoStmt->fetchAll()
        );
        $hasPaymentDateColumn = in_array('payment_date', $tableColumns, true);

        $statusStmt = $pdo->query("SELECT DISTINCT TRIM(status) AS status_value FROM webhook_events WHERE TRIM(IFNULL(status, '')) <> '' ORDER BY status_value ASC");
        $statusOptions = array_values(array_filter(array_map(static fn(array $row): string => (string)($row['status_value'] ?? ''), $statusStmt->fetchAll()), static fn(string $value): bool => $value !== ''));

        $eventTypeStmt = $pdo->query("SELECT DISTINCT TRIM(event_type) AS event_type_value FROM webhook_events WHERE TRIM(IFNULL(event_type, '')) <> '' ORDER BY event_type_value ASC");
        $eventTypeOptions = array_values(array_filter(array_map(static fn(array $row): string => (string)($row['event_type_value'] ?? ''), $eventTypeStmt->fetchAll()), static fn(string $value): bool => $value !== ''));

        $whereConditions = ["TRIM(IFNULL(status, '')) <> ''"];
        $params = [];

        $paymentDateSources = [];
        if ($hasPaymentDateColumn) {
            $paymentDateSources[] = "NULLIF(payment_date, '')";
        }
        $paymentDateSources[] = "NULLIF(json_extract(raw_json, '$.\\\"入金日\\\"'), '')";
        $paymentDateSources[] = "NULLIF(json_extract(raw_json, '$.data.created_on'), '')";
        $paymentDateSources[] = "received_at";
        $paymentDateExpression = 'date(COALESCE(' . implode(', ', $paymentDateSources) . '))';

        if ($filters['payment_date_from'] !== '') {
            $whereConditions[] = "{$paymentDateExpression} >= :payment_date_from";
            $params[':payment_date_from'] = $filters['payment_date_from'];
        }
        if ($filters['payment_date_to'] !== '') {
            $whereConditions[] = "{$paymentDateExpression} <= :payment_date_to";
            $params[':payment_date_to'] = $filters['payment_date_to'];
        }
        if ($filters['payer_name'] !== '') {
            $whereConditions[] = "raw_json LIKE :payer_name";
            $params[':payer_name'] = '%' . $filters['payer_name'] . '%';
        }
        if ($filters['email'] !== '') {
            $whereConditions[] = "raw_json LIKE :email";
            $params[':email'] = '%' . $filters['email'] . '%';
        }
        if ($filters['event_type'] !== '') {
            $whereConditions[] = "event_type = :event_type";
            $params[':event_type'] = $filters['event_type'];
        }
        if ($filters['status'] !== '') {
            $whereConditions[] = "status = :status";
            $params[':status'] = $filters['status'];
        }
        $whereSql = 'WHERE ' . implode(' AND ', $whereConditions);

        $countStmt = $pdo->prepare("SELECT COUNT(*) FROM webhook_events {$whereSql}");
        $countStmt->execute($params);
        $totalCount = (int)$countStmt->fetchColumn();

        $summaryStmt = $pdo->prepare("SELECT COUNT(*) AS total_count, COALESCE(SUM(amount), 0) AS total_amount FROM webhook_events {$whereSql}");
        $summaryStmt->execute($params);
        $summary = $summaryStmt->fetch();
        if (is_array($summary)) {
            $summaryCount = (int)($summary['total_count'] ?? 0);
            $summaryAmount = (int)($summary['total_amount'] ?? 0);
        }
        $totalPages = max(1, (int)ceil($totalCount / PER_PAGE));
        if ($page > $totalPages) {
            $page = $totalPages;
        }

        $offset = ($page - 1) * PER_PAGE;

        $selectPaymentDate = $hasPaymentDateColumn ? 'payment_date,' : "NULL AS payment_date,";

        $sql = "SELECT
                    id,
                    received_at,
                    {$selectPaymentDate}
                    raw_json,
                    event_type,
                    status
                FROM webhook_events
                {$whereSql}
                ORDER BY datetime(received_at) DESC, id DESC
                LIMIT :limit OFFSET :offset";

        $stmt = $pdo->prepare($sql);
        foreach ($params as $key => $value) {
            $stmt->bindValue($key, $value, PDO::PARAM_STR);
        }
        $stmt->bindValue(':limit', PER_PAGE, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll();

        if ($totalCount === 0) {
            $infoMessage = '検索条件に一致するデータがありません。';
        }
    } catch (Throwable $e) {
        $errorMessage = 'DBの読み込みに失敗しました。';
    }
} else {
    $infoMessage = 'DBファイルがまだ存在しないため、表示するデータがありません。';
}

require __DIR__ . '/header.php';
?>

<div class="content-layout">
<aside class="panel sidebar-panel" id="sidebarPanel">
    <h2>メニュー</h2>
    <div class="sidebar-brand">
        <a class="sidebar-brand-link" href="index.php">
            <img src="img/icon.png" alt="UNIVA-MIRROR アイコン" class="sidebar-brand-icon">
            <span class="sidebar-brand-title">UNIVA-MIRROR</span>
        </a>
    </div>
    <nav aria-label="サイドバー">
        <a class="sidebar-link" href="calc.php">日別・月別集計</a>
        <a class="sidebar-link" href="calc.php">入金集計カード</a>
    </nav>
</aside>
<button type="button" class="sidebar-overlay" id="sidebarOverlay" aria-label="サイドバーを閉じる"></button>


<div class="content-main">
<section class="panel search-panel">
    <form method="get" class="search-form">
        <div class="search-grid">
            <div class="search-group">
                <label for="payment_date_from">入金日（開始日）</label>
                <input type="date" id="payment_date_from" name="payment_date_from" value="<?= h($filters['payment_date_from']) ?>">
            </div>
            <div class="search-group">
                <label for="payment_date_to">入金日（終了日）</label>
                <input type="date" id="payment_date_to" name="payment_date_to" value="<?= h($filters['payment_date_to']) ?>">
            </div>
            <div class="search-group">
                <label for="payer_name">入金者名（部分一致）</label>
                <input type="text" id="payer_name" name="payer_name" value="<?= h($filters['payer_name']) ?>" placeholder="例: 山田">
            </div>
            <div class="search-group">
                <label for="email">メールアドレス</label>
                <input type="text" id="email" name="email" value="<?= h($filters['email']) ?>" placeholder="例: sample@example.com">
            </div>
            <div class="search-group">
                <label for="status">ステータス</label>
                <select id="status" name="status">
                    <option value="">すべて</option>
                    <?php foreach ($statusOptions as $statusOption): ?>
                        <option value="<?= h($statusOption) ?>" <?= $statusOption === $filters['status'] ? 'selected' : '' ?>><?= h($statusOption) ?></option>
                    <?php endforeach; ?>
                </select>
            </div>
            <div class="search-group">
                <label for="event_type">イベント</label>
                <select id="event_type" name="event_type">
                    <option value="">すべて</option>
                    <?php foreach ($eventTypeOptions as $eventTypeOption): ?>
                        <option value="<?= h($eventTypeOption) ?>" <?= $eventTypeOption === $filters['event_type'] ? 'selected' : '' ?>><?= h($eventTypeOption) ?></option>
                    <?php endforeach; ?>
                </select>
            </div>
        </div>
        <div class="search-actions">
            <button type="submit" class="btn btn-primary">検索</button>
            <a href="index.php" class="btn btn-secondary">リセット</a>
        </div>
    </form>
</section>


<?php if ($errorMessage !== ''): ?>
    <section class="panel message-panel error">
        <p><?= h($errorMessage) ?></p>
    </section>
<?php endif; ?>

<?php if ($infoMessage !== '' && $errorMessage === ''): ?>
    <section class="panel message-panel info">
        <p><?= h($infoMessage) ?></p>
    </section>
<?php endif; ?>

<section class="panel table-panel">
    <div class="table-header">
        <div>
            <h3>一覧表示</h3>
            <p>入金日 / 入金額 / 入金者名 / メールアドレス / イベント / ステータス（最大25件ずつ）</p>
        </div>
        <div class="table-meta">
            <span>ページ <?= h((string)$page) ?> / <?= h((string)$totalPages) ?></span>
        </div>
    </div>

    <div class="table-wrap">
        <table class="data-table">
            <thead>
                <tr>
                    <th>入金日</th>
                    <th>入金額</th>
                    <th>入金者名</th>
                    <th>メールアドレス</th>
                    <th>イベント</th>
                    <th>ステータス</th>
                </tr>
            </thead>
            <tbody>
                <?php if (!empty($rows)): ?>
                    <?php foreach ($rows as $row): ?>
                        <?php $display = extract_display_data($row); ?>
                        <tr>
                            <td><?= h((string)$display['payment_date']) ?></td>
                            <td><?= h(format_jpy_amount($display['payment_amount'])) ?></td>
                            <td><?= h((string)$display['payer_name']) ?></td>
                            <td><?= h((string)$display['email']) ?></td>
                            <td><?= h((string)$display['event_type']) ?></td>
                            <td>
                                <span class="status-badge <?= h(status_badge_class((string)$display['status'])) ?>">
                                    <?= h((string)$display['status']) ?>
                                </span>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php else: ?>
                    <tr>
                        <td colspan="6" class="empty-cell">表示するデータがありません。</td>
                    </tr>
                <?php endif; ?>
            </tbody>
        </table>
    </div>

    <nav class="pagination" aria-label="ページネーション">
        <?php $prevPage = max(1, $page - 1); ?>
        <?php $nextPage = min($totalPages, $page + 1); ?>

        <a class="page-link <?= $page <= 1 ? 'disabled' : '' ?>" href="<?= $page <= 1 ? '#' : h(build_page_url($prevPage, $filters)) ?>">前へ</a>

        <?php
        $start = max(1, $page - 2);
        $end = min($totalPages, $page + 2);
        for ($i = $start; $i <= $end; $i++):
        ?>
            <a class="page-link <?= $i === $page ? 'active' : '' ?>" href="<?= h(build_page_url($i, $filters)) ?>"><?= $i ?></a>
        <?php endfor; ?>

        <a class="page-link <?= $page >= $totalPages ? 'disabled' : '' ?>" href="<?= $page >= $totalPages ? '#' : h(build_page_url($nextPage, $filters)) ?>">次へ</a>
    </nav>
</section>

</div>
</div>
<?php require __DIR__ . '/footer.php'; ?>
