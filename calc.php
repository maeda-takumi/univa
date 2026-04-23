<?php
declare(strict_types=1);

$pageTitle = '入金集計カード';

const DB_FILE = __DIR__ . '/data/univapay_webhook.sqlite';
const DETAIL_PER_PAGE = 50;

function h(?string $value): string
{
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function format_jpy_amount(mixed $amount): string
{
    if ($amount === null) {
        return '¥0';
    }

    return '¥' . number_format((int)round((float)$amount));
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

function extract_display_data(array $row): array
{
    $payload = json_decode((string)($row['raw_json'] ?? ''), true);
    if (!is_array($payload)) {
        $payload = [];
    }

    $paymentDate = first_non_empty_value([
        $payload['入金日'] ?? null,
        $payload['イベント作成日時'] ?? null,
        $payload['課金作成日時'] ?? null,
        $row['received_at'] ?? null,
    ]);

    $paymentAmount = first_non_empty_value([
        $payload['入金額'] ?? null,
        $payload['課金金額'] ?? null,
        $payload['イベント金額'] ?? null,
        (string)($row['amount'] ?? ''),
    ]);

    $payerName = first_non_empty_value([
        $payload['入金者名'] ?? null,
        $payload['氏名'] ?? null,
        $payload['カード名義'] ?? null,
    ]);

    return [
        'payment_date' => $paymentDate ?? '',
        'payment_amount' => $paymentAmount ?? '',
        'payer_name' => $payerName ?? '',
        'status' => trim((string)($row['status'] ?? '')),
    ];
}

$groupBy = ($_GET['group_by'] ?? 'day') === 'month' ? 'month' : 'day';
$selectedBucket = trim((string)($_GET['bucket'] ?? ''));
$detailPage = max(1, (int)($_GET['detail_page'] ?? 1));

$errorMessage = '';
$cards = [];
$detailRows = [];
$detailTotalCount = 0;
$detailTotalPages = 1;

if (file_exists(DB_FILE)) {
    try {
        $pdo = new PDO('sqlite:' . DB_FILE);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

        $dateExpr = "date(COALESCE(NULLIF(json_extract(raw_json, '$.\\\"入金日\\\"'), ''), received_at))";
        $bucketExpr = $groupBy === 'month'
            ? "strftime('%Y-%m', {$dateExpr})"
            : $dateExpr;

        $cardStmt = $pdo->query("SELECT {$bucketExpr} AS bucket, COUNT(*) AS total_count, COALESCE(SUM(amount), 0) AS total_amount FROM webhook_events WHERE TRIM(IFNULL(status, '')) <> '' GROUP BY bucket ORDER BY bucket DESC");
        $cards = $cardStmt->fetchAll();

        if ($selectedBucket !== '') {
            $whereBucket = $groupBy === 'month'
                ? "strftime('%Y-%m', {$dateExpr}) = :bucket"
                : "{$dateExpr} = :bucket";

            $countStmt = $pdo->prepare("SELECT COUNT(*) FROM webhook_events WHERE TRIM(IFNULL(status, '')) <> '' AND {$whereBucket}");
            $countStmt->execute([':bucket' => $selectedBucket]);
            $detailTotalCount = (int)$countStmt->fetchColumn();
            $detailTotalPages = max(1, (int)ceil($detailTotalCount / DETAIL_PER_PAGE));
            if ($detailPage > $detailTotalPages) {
                $detailPage = $detailTotalPages;
            }

            $offset = ($detailPage - 1) * DETAIL_PER_PAGE;
            $detailStmt = $pdo->prepare("SELECT id, received_at, raw_json, amount, status FROM webhook_events WHERE TRIM(IFNULL(status, '')) <> '' AND {$whereBucket} ORDER BY datetime(received_at) DESC, id DESC LIMIT :limit OFFSET :offset");
            $detailStmt->bindValue(':bucket', $selectedBucket, PDO::PARAM_STR);
            $detailStmt->bindValue(':limit', DETAIL_PER_PAGE, PDO::PARAM_INT);
            $detailStmt->bindValue(':offset', $offset, PDO::PARAM_INT);
            $detailStmt->execute();
            $detailRows = $detailStmt->fetchAll();
        }
    } catch (Throwable $e) {
        $errorMessage = '集計データの読み込みに失敗しました。';
    }
} else {
    $errorMessage = 'DBファイルが存在しません。';
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
    <form method="get" class="search-form inline-filter">
        <div class="search-group filter-group">
            <label for="group_by">集計単位</label>
            <select id="group_by" name="group_by" onchange="this.form.submit()">
                <option value="day" <?= $groupBy === 'day' ? 'selected' : '' ?>>日別</option>
                <option value="month" <?= $groupBy === 'month' ? 'selected' : '' ?>>月別</option>
            </select>
        </div>
    </form>
</section>

<?php if ($errorMessage !== ''): ?>
    <section class="panel message-panel error"><p><?= h($errorMessage) ?></p></section>
<?php endif; ?>

<section class="panel table-panel">
    <div class="table-header">
        <div>
            <h3><?= $groupBy === 'day' ? '日別' : '月別' ?> 入金集計カード</h3>
            <p>カードをクリックすると該当データを一覧表示します。</p>
        </div>
    </div>
    <div class="summary-cards">
        <?php if (!empty($cards)): ?>
            <?php foreach ($cards as $card): ?>
                <?php
                    $bucket = (string)($card['bucket'] ?? '');
                    $link = 'calc.php?' . http_build_query(['group_by' => $groupBy, 'bucket' => $bucket]);
                ?>
                <a class="summary-card-link <?= $selectedBucket === $bucket ? 'active' : '' ?>" href="<?= h($link) ?>">
                    <span class="summary-card-period"><?= h($bucket) ?></span>
                    <span class="summary-card-amount"><?= h(format_jpy_amount($card['total_amount'] ?? 0)) ?></span>
                    <span class="summary-card-count"><?= h(number_format((int)($card['total_count'] ?? 0))) ?>件</span>
                </a>
            <?php endforeach; ?>
        <?php else: ?>
            <p>表示できる集計データがありません。</p>
        <?php endif; ?>
    </div>
</section>

<?php if ($selectedBucket !== ''): ?>
<section class="panel table-panel">
    <div class="table-header">
        <div>
            <h3>抽出結果: <?= h($selectedBucket) ?></h3>
            <p><?= h(number_format($detailTotalCount)) ?>件（<?= h((string)$detailPage) ?>/<?= h((string)$detailTotalPages) ?>ページ）</p>
        </div>
    </div>
    <div class="table-wrap">
        <table class="data-table">
            <thead>
                <tr>
                    <th>入金日</th>
                    <th>入金額</th>
                    <th>入金者名</th>
                    <th>ステータス</th>
                </tr>
            </thead>
            <tbody>
                <?php if (!empty($detailRows)): ?>
                    <?php foreach ($detailRows as $row): ?>
                        <?php $display = extract_display_data($row); ?>
                        <tr>
                            <td><?= h((string)$display['payment_date']) ?></td>
                            <td><?= h(format_jpy_amount($display['payment_amount'])) ?></td>
                            <td><?= h((string)$display['payer_name']) ?></td>
                            <td><?= h((string)$display['status']) ?></td>
                        </tr>
                    <?php endforeach; ?>
                <?php else: ?>
                    <tr><td colspan="4" class="empty-cell">該当データがありません。</td></tr>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>
</div>
</div>
<?php require __DIR__ . '/footer.php';
