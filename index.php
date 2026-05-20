<?php
$dbFiles = glob(__DIR__ . '/*.sqlite') ?: [];
$records = [];

$filters = [
    'date_from' => trim((string)($_GET['date_from'] ?? '')),
    'date_to' => trim((string)($_GET['date_to'] ?? '')),
    'status' => trim((string)($_GET['status'] ?? '')),
    'payment_type' => trim((string)($_GET['payment_type'] ?? '')),
    'name' => trim((string)($_GET['name'] ?? '')),
    'email' => trim((string)($_GET['email'] ?? '')),
];
foreach ($dbFiles as $file) {
    $pdo = null;
    try {
        $pdo = new PDO('sqlite:' . $file, null, null, [
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

        if ($table === null) {
            continue;
        }

        $stmt = $pdo->query(
            'SELECT created_on, status, payment_type, amount, metadata_name, cardholder_name, cardholder_email FROM ' . $table . ' ORDER BY created_on DESC'
        );

        while ($row = $stmt->fetch()) {
            $metadataName = trim((string)($row['metadata_name'] ?? ''));
            $cardholderName = trim((string)($row['cardholder_name'] ?? ''));
            $records[] = [
                'created_on' => $row['created_on'] ?? '',
                'status' => $row['status'] ?? '',
                'payment_type' => $row['payment_type'] ?? '',
                'amount' => $row['amount'] ?? '',
                'metadata_name' => $metadataName !== '' ? $metadataName : $cardholderName,
                'cardholder_email' => $row['cardholder_email'] ?? '',
            ];
        }
    } catch (Throwable $e) {
        // transactions テーブルが存在しない、または読み込み不可な sqlite はスキップ
        continue;
    }
}

$records = array_values(array_filter($records, static function (array $record) use ($filters): bool {
    $recordDateRaw = trim((string)($record['created_on'] ?? ''));
    $recordDate = $recordDateRaw !== '' ? strtotime($recordDateRaw) : false;

    if ($filters['date_from'] !== '') {
        $from = strtotime($filters['date_from'] . ' 00:00:00');
        if ($from !== false && ($recordDate === false || $recordDate < $from)) {
            return false;
        }
    }

    if ($filters['date_to'] !== '') {
        $to = strtotime($filters['date_to'] . ' 23:59:59');
        if ($to !== false && ($recordDate === false || $recordDate > $to)) {
            return false;
        }
    }

    if ($filters['status'] !== '' && (string)$record['status'] !== $filters['status']) {
        return false;
    }

    if ($filters['payment_type'] !== '' && (string)$record['payment_type'] !== $filters['payment_type']) {
        return false;
    }

    if ($filters['name'] !== '') {
        $name = mb_strtolower((string)($record['metadata_name'] ?? ''));
        if (!str_contains($name, mb_strtolower($filters['name']))) {
            return false;
        }
    }

    if ($filters['email'] !== '') {
        $email = mb_strtolower((string)($record['cardholder_email'] ?? ''));
        if (!str_contains($email, mb_strtolower($filters['email']))) {
            return false;
        }
    }

    return true;
}));
if (isset($_GET['export']) && $_GET['export'] === 'csv') {
    $filename = 'transaction_history_' . date('Ymd_His') . '.csv';
    header('Content-Type: text/csv; charset=UTF-8');
    header('Content-Disposition: attachment; filename="' . $filename . '"');

    $output = fopen('php://output', 'w');
    if ($output !== false) {
        fwrite($output, "\xEF\xBB\xBF");
        fputcsv($output, ['日付', '状態', '入金方法', '入金額', '氏名', 'メールアドレス']);
        foreach ($records as $record) {
            fputcsv($output, [
                formatDate((string)$record['created_on']),
                labelStatus((string)$record['status']),
                labelPaymentType((string)$record['payment_type']),
                formatAmount($record['amount']),
                (string)$record['metadata_name'],
                (string)$record['cardholder_email'],
            ]);
        }
        fclose($output);
    }
    exit;
}
include __DIR__ . '/header.php';

function formatDate(string $value): string
{
    if (trim($value) === '') {
        return '';
    }

    $timestamp = strtotime($value);
    if ($timestamp === false) {
        return $value;
    }

    return date('Y/m/d H:i:s', $timestamp);
}

function formatAmount($value): string
{
    if ($value === null || $value === '') {
        return '';
    }

    if (!is_numeric($value)) {
        return (string)$value;
    }

    return number_format((float)$value);
}

function labelStatus(string $status): string
{
    return match ($status) {
        'successful' => '成功',
        'failed' => '失敗',
        'awaiting' => '処理待ち',
        default => $status,
    };
}

function labelPaymentType(string $paymentType): string
{
    return match ($paymentType) {
        'bank_transfer' => '振込',
        'card' => 'カード',
        default => $paymentType,
    };
}
?>

<section class="panel">
  <div class="panel-head">
    <h2>取引一覧</h2>
    <div class="panel-actions">
      <span class="count"><?= count($records); ?> 件</span>
      <a
        class="action-btn action-btn-secondary"
        href="?<?= htmlspecialchars(http_build_query(array_merge($filters, ['export' => 'csv'])), ENT_QUOTES, 'UTF-8'); ?>"
      >CSVエクスポート</a>
    </div>
  </div>

  <form method="get" class="filter-form">
    <div>
      <label for="date_from">日付（開始）</label>
      <input type="date" id="date_from" name="date_from" value="<?= htmlspecialchars($filters['date_from'], ENT_QUOTES, 'UTF-8'); ?>">
    </div>
    <div>
      <label for="date_to">日付（終了）</label>
      <input type="date" id="date_to" name="date_to" value="<?= htmlspecialchars($filters['date_to'], ENT_QUOTES, 'UTF-8'); ?>">
    </div>
    <div>
      <label for="status">状態</label>
      <select id="status" name="status">
        <option value="">すべて</option>
        <option value="successful" <?= $filters['status'] === 'successful' ? 'selected' : ''; ?>>成功</option>
        <option value="failed" <?= $filters['status'] === 'failed' ? 'selected' : ''; ?>>失敗</option>
        <option value="awaiting" <?= $filters['status'] === 'awaiting' ? 'selected' : ''; ?>>処理待ち</option>
      </select>
    </div>
    <div>
      <label for="payment_type">入金方法</label>
      <select id="payment_type" name="payment_type">
        <option value="">すべて</option>
        <option value="bank_transfer" <?= $filters['payment_type'] === 'bank_transfer' ? 'selected' : ''; ?>>振込</option>
        <option value="card" <?= $filters['payment_type'] === 'card' ? 'selected' : ''; ?>>カード</option>
      </select>
    </div>
    <div>
      <label for="name">氏名</label>
      <input type="text" id="name" name="name" value="<?= htmlspecialchars($filters['name'], ENT_QUOTES, 'UTF-8'); ?>">
    </div>
    <div>
      <label for="email">メールアドレス</label>
      <input type="text" id="email" name="email" value="<?= htmlspecialchars($filters['email'], ENT_QUOTES, 'UTF-8'); ?>">
    </div>
    <div>
      <button type="submit" class="action-btn action-btn-primary">実行</button>
      <a href="index.php" class="action-btn action-btn-reset">リセット</a>
    </div>
  </form>
  <?php if (empty($records)): ?>
    <div class="empty-state">
      <p>表示可能な取引データは見つかりませんでした。</p>
      <p class="hint">.sqlite ファイルの transactions テーブルをご確認ください。</p>
    </div>
  <?php else: ?>
    <div class="list-scroll">
      <ul class="transaction-list">
        <li class="transaction-item transaction-head" aria-hidden="true">
          <span>日付</span>
          <span>状態</span>
          <span>入金方法</span>
          <span>入金額</span>
          <span>氏名</span>
          <span>メールアドレス</span>
        </li>
        <?php foreach ($records as $record): ?>
          <li class="transaction-item">
            <span><?= htmlspecialchars(formatDate((string)$record['created_on']), ENT_QUOTES, 'UTF-8'); ?></span>
            <span>
              <span class="status-badge status-badge-<?= htmlspecialchars((string)$record['status'], ENT_QUOTES, 'UTF-8'); ?>">
                <?= htmlspecialchars(labelStatus((string)$record['status']), ENT_QUOTES, 'UTF-8'); ?>
              </span>
            </span>
            <span><?= htmlspecialchars(labelPaymentType((string)$record['payment_type']), ENT_QUOTES, 'UTF-8'); ?></span>
            <span><?= htmlspecialchars(formatAmount($record['amount']), ENT_QUOTES, 'UTF-8'); ?></span>
            <span><?= htmlspecialchars((string)$record['metadata_name'], ENT_QUOTES, 'UTF-8'); ?></span>
            <span><?= htmlspecialchars((string)$record['cardholder_email'], ENT_QUOTES, 'UTF-8'); ?></span>
          </li>
        <?php endforeach; ?>
      </ul>
    </div>
  <?php endif; ?>
</section>

<?php include __DIR__ . '/footer.php'; ?>
