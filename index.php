<?php
$dbFiles = glob(__DIR__ . '/*.sqlite') ?: [];
include __DIR__ . '/header.php';
?>

<section class="panel">
  <div class="panel-head">
    <h2>利用可能なDBファイル</h2>
    <span class="count"><?= count($dbFiles); ?> 件</span>
  </div>

  <?php if (empty($dbFiles)): ?>
    <div class="empty-state">
      <p>このディレクトリに .sqlite ファイルは見つかりませんでした。</p>
      <p class="hint">インポートボタンからデータ登録を行ってください。</p>
    </div>
  <?php else: ?>
    <ul class="db-grid">
      <?php foreach ($dbFiles as $file): ?>
        <?php
          $name = basename($file);
          $size = filesize($file);
          $updated = date('Y-m-d H:i', filemtime($file));
        ?>
        <li class="db-card">
          <h3><?= htmlspecialchars($name, ENT_QUOTES, 'UTF-8'); ?></h3>
          <dl>
            <div>
              <dt>サイズ</dt>
              <dd><?= number_format((float)$size / 1024, 1); ?> KB</dd>
            </div>
            <div>
              <dt>更新日時</dt>
              <dd><?= $updated; ?></dd>
            </div>
          </dl>
        </li>
      <?php endforeach; ?>
    </ul>
  <?php endif; ?>
</section>

<?php include __DIR__ . '/footer.php'; ?>
