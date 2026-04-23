<?php
declare(strict_types=1);

$pageTitle = '入金データ一覧';

const DB_FILE = __DIR__ . '/data/univapay_webhook.sqlite';
const PER_PAGE = 25;

function h(?string $value): string
{
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function build_page_url(int $page, string $keyword): string
{
    $params = ['page' => $page];
    if ($keyword !== '') {
        $params['q'] = $keyword;
    }
    return '?' . http_build_query($params);
}

$keyword = trim((string)($_GET['q'] ?? ''));
$page = max(1, (int)($_GET['page'] ?? 1));

$dbExists = file_exists(DB_FILE);
$rows = [];
$totalCount = 0;
$totalPages = 1;
$errorMessage = '';
$infoMessage = '';

if ($dbExists) {
    try {
        $pdo = new PDO('sqlite:' . DB_FILE);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

        $whereSql = '';
        $params = [];

        if ($keyword !== '') {
            $whereSql = "WHERE
                received_at LIKE :kw OR
                event_type LIKE :kw OR
                status LIKE :kw OR
                transaction_id LIKE :kw OR
                charge_id LIKE :kw OR
                store_id LIKE :kw OR
                customer_id LIKE :kw OR
                currency LIKE :kw OR
                raw_json LIKE :kw";
            $params[':kw'] = '%' . $keyword . '%';
        }

        $countStmt = $pdo->prepare("SELECT COUNT(*) FROM webhook_events {$whereSql}");
        $countStmt->execute($params);
        $totalCount = (int)$countStmt->fetchColumn();

        $totalPages = max(1, (int)ceil($totalCount / PER_PAGE));
        if ($page > $totalPages) {
            $page = $totalPages;
        }

        $offset = ($page - 1) * PER_PAGE;

        $sql = "SELECT
                    id,
                    received_at,
                    event_type,
                    status,
                    transaction_id,
                    charge_id,
                    store_id,
                    customer_id,
                    amount,
                    currency,
                    livemode
                FROM webhook_events
                {$whereSql}
                ORDER BY id DESC
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
            $infoMessage = $keyword !== '' ? '検索条件に一致するデータがありません。' : 'まだデータがありません。';
        }
    } catch (Throwable $e) {
        $errorMessage = 'DBの読み込みに失敗しました。';
    }
} else {
    $infoMessage = 'DBファイルがまだ存在しないため、表示するデータがありません。';
}

require __DIR__ . '/header.php';
?>

<section class="panel search-panel">
    <form method="get" class="search-form">
        <div class="search-group">
            <label for="q">検索</label>
            <input
                type="text"
                id="q"
                name="q"
                value="<?= h($keyword) ?>"
                placeholder="イベント種別、取引ID、ステータスなどで検索"
            >
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
            <p>最大25件ずつ表示</p>
        </div>
        <div class="table-meta">
            <span>ページ <?= h((string)$page) ?> / <?= h((string)$totalPages) ?></span>
        </div>
    </div>

    <div class="table-wrap">
        <table class="data-table">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>受信日時</th>
                    <th>イベント</th>
                    <th>ステータス</th>
                    <th>取引ID</th>
                    <th>Charge ID</th>
                    <th>Store ID</th>
                    <th>Customer ID</th>
                    <th>金額</th>
                    <th>通貨</th>
                    <th>本番</th>
                </tr>
            </thead>
            <tbody>
                <?php if (!empty($rows)): ?>
                    <?php foreach ($rows as $row): ?>
                        <tr>
                            <td><?= h((string)$row['id']) ?></td>
                            <td><?= h((string)$row['received_at']) ?></td>
                            <td><span class="pill"><?= h((string)$row['event_type']) ?></span></td>
                            <td><?= h((string)$row['status']) ?></td>
                            <td class="mono"><?= h((string)$row['transaction_id']) ?></td>
                            <td class="mono"><?= h((string)$row['charge_id']) ?></td>
                            <td class="mono"><?= h((string)$row['store_id']) ?></td>
                            <td class="mono"><?= h((string)$row['customer_id']) ?></td>
                            <td><?= $row['amount'] !== null ? number_format((int)$row['amount']) : '' ?></td>
                            <td><?= h((string)$row['currency']) ?></td>
                            <td><?= ((int)$row['livemode'] === 1) ? 'Yes' : (((string)$row['livemode'] === '') ? '' : 'No') ?></td>
                        </tr>
                    <?php endforeach; ?>
                <?php else: ?>
                    <tr>
                        <td colspan="11" class="empty-cell">表示するデータがありません。</td>
                    </tr>
                <?php endif; ?>
            </tbody>
        </table>
    </div>

    <nav class="pagination" aria-label="ページネーション">
        <?php $prevPage = max(1, $page - 1); ?>
        <?php $nextPage = min($totalPages, $page + 1); ?>

        <a class="page-link <?= $page <= 1 ? 'disabled' : '' ?>" href="<?= $page <= 1 ? '#' : h(build_page_url($prevPage, $keyword)) ?>">前へ</a>

        <?php
        $start = max(1, $page - 2);
        $end = min($totalPages, $page + 2);
        for ($i = $start; $i <= $end; $i++):
        ?>
            <a class="page-link <?= $i === $page ? 'active' : '' ?>" href="<?= h(build_page_url($i, $keyword)) ?>"><?= $i ?></a>
        <?php endfor; ?>

        <a class="page-link <?= $page >= $totalPages ? 'disabled' : '' ?>" href="<?= $page >= $totalPages ? '#' : h(build_page_url($nextPage, $keyword)) ?>">次へ</a>
    </nav>
</section>

<?php require __DIR__ . '/footer.php'; ?>
