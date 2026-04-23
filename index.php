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

    $paymentDate = first_non_empty_value([
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
        get_nested_value($payload, ['customer', 'name']),
        get_nested_value($payload, ['data', 'customer_name']),
    ]);

    $email = first_non_empty_value([
        $payload['メールアドレス'] ?? null,
        get_nested_value($payload, ['customer', 'email']),
        get_nested_value($payload, ['data', 'email']),
        $tokenMetadata['email'] ?? null,
        $chargeMetadata['email'] ?? null,
    ]);

    return [
        'payment_date' => $paymentDate ?? '',
        'payment_amount' => $paymentAmount ?? '',
        'payer_name' => $payerName ?? '',
        'email' => $email ?? '',
    ];
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
$summaryCount = 0;
$summaryAmount = 0;

if ($dbExists) {
    try {
        $pdo = new PDO('sqlite:' . DB_FILE);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

        $whereConditions = [
            "lower(ifnull(status, '')) IN ('successful', 'succeeded', 'success', 'paid', 'completed', '成功')",
        ];
        $params = [];

        if ($keyword !== '') {
            $whereConditions[] = "(
                received_at LIKE :kw OR
                raw_json LIKE :kw
            )";
            $params[':kw'] = '%' . $keyword . '%';
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

        $sql = "SELECT
                    id,
                    received_at,
                    raw_json
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

<?php if ($errorMessage === ''): ?>
    <section class="panel summary-panel">
        <h3>成功ステータス集計</h3>
        <div class="summary-grid">
            <div class="summary-item">
                <span class="summary-label">対象件数</span>
                <span class="summary-value"><?= h(number_format($summaryCount)) ?> 件</span>
            </div>
            <div class="summary-item">
                <span class="summary-label">合計金額</span>
                <span class="summary-value"><?= h(number_format($summaryAmount)) ?></span>
            </div>
        </div>
    </section>
<?php endif; ?>

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
            <p>入金日 / 入金額 / 入金者名 / メールアドレス（最大25件ずつ）</p>
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
                </tr>
            </thead>
            <tbody>
                <?php if (!empty($rows)): ?>
                    <?php foreach ($rows as $row): ?>
                        <?php $display = extract_display_data($row); ?>
                        <tr>
                            <td><?= h((string)$display['payment_date']) ?></td>
                            <td><?= h((string)$display['payment_amount']) ?></td>
                            <td><?= h((string)$display['payer_name']) ?></td>
                            <td><?= h((string)$display['email']) ?></td>
                        </tr>
                    <?php endforeach; ?>
                <?php else: ?>
                    <tr>
                        <td colspan="4" class="empty-cell">表示するデータがありません。</td>
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
