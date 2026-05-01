<?php
$dbFiles = glob(__DIR__ . '/*.sqlite') ?: [];
$records = [];

foreach ($dbFiles as $file) {
    $pdo = null;
    try {
        $pdo = new PDO('sqlite:' . $file, null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);

        $stmt = $pdo->query(
            'SELECT created_on, status, payment_type, metadata_name, cardholder_name, cardholder_email FROM transactions ORDER BY created_on DESC'
        );

        while ($row = $stmt->fetch()) {
            $metadataName = trim((string)($row['metadata_name'] ?? ''));
            $cardholderName = trim((string)($row['cardholder_name'] ?? ''));
            $records[] = [
                'created_on' => $row['created_on'] ?? '',
                'status' => $row['status'] ?? '',
                'payment_type' => $row['payment_type'] ?? '',
                'metadata_name' => $metadataName !== '' ? $metadataName : $cardholderName,
                'cardholder_email' => $row['cardholder_email'] ?? '',
            ];
        }
    } catch (Throwable $e) {
        // transactions テーブルが存在しない、または読み込み不可な sqlite はスキップ
        continue;
    }
}
include __DIR__ . '/header.php';
?>

<section class="panel">
  <div class="panel-head">
    <h2>取引一覧</h2>
    <span class="count"><?= count($records); ?> 件</span>
  </div>

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
          <span>氏名</span>
          <span>メールアドレス</span>
        </li>
        <?php foreach ($records as $record): ?>
          <li class="transaction-item">
            <span><?= htmlspecialchars((string)$record['created_on'], ENT_QUOTES, 'UTF-8'); ?></span>
            <span><?= htmlspecialchars((string)$record['status'], ENT_QUOTES, 'UTF-8'); ?></span>
            <span><?= htmlspecialchars((string)$record['payment_type'], ENT_QUOTES, 'UTF-8'); ?></span>
            <span><?= htmlspecialchars((string)$record['metadata_name'], ENT_QUOTES, 'UTF-8'); ?></span>
            <span><?= htmlspecialchars((string)$record['cardholder_email'], ENT_QUOTES, 'UTF-8'); ?></span>
          </li>
        <?php endforeach; ?>
      </ul>
    </div>
  <?php endif; ?>
</section>

<?php include __DIR__ . '/footer.php'; ?>
