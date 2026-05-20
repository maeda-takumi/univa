<?php
$syncMetaPath = __DIR__ . '/nyukin/data/sync_meta.json';
$syncMeta = is_file($syncMetaPath) ? json_decode((string)file_get_contents($syncMetaPath), true) : [];
$syncStatus = (string)($_GET['sync'] ?? '');
$univapaySyncStatus = (string)($_GET['univapay_sync'] ?? '');
$syncMessage = '';
$univapaySyncMessage = '';
if ($syncStatus === 'success') {
    $syncMessage = '自社DBを更新しました。'
        . (isset($_GET['count']) ? '取込件数: ' . (int)$_GET['count'] . '件' : '')
        . (isset($_GET['payments']) ? ' / 入金行: ' . (int)$_GET['payments'] . '件' : '');
} elseif ($syncStatus === 'error') {
    $syncMessage = '自社DBの更新に失敗しました。前回同期データを表示しています。';
}
if ($univapaySyncStatus === 'success') {
    $univapaySyncMessage = 'UnivaPay APIを実行しました。取得件数: '
        . (int)($_GET['univapay_count'] ?? 0)
        . '件 / 保存件数: '
        . (int)($_GET['univapay_saved'] ?? 0)
        . '件';
} elseif ($univapaySyncStatus === 'error') {
    $univapaySyncMessage = 'UnivaPay APIの実行に失敗しました。';
}
$lastSyncedAt = is_array($syncMeta) ? (string)($syncMeta['synced_at'] ?? '') : '';
$lastSyncedLabel = '';
if ($lastSyncedAt !== '') {
    try {
        $lastSyncedLabel = (new DateTimeImmutable($lastSyncedAt))->setTimezone(new DateTimeZone('Asia/Tokyo'))->format('Y/m/d H:i:s');
    } catch (Throwable $e) {
        $lastSyncedLabel = $lastSyncedAt;
    }
}
?>
<!doctype html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>&#20837;&#37329;&#24773;&#22577; | UnivaPay</title>
  <link rel="stylesheet" href="style/style.css?v=<?= time(); ?>">
</head>
<body>
  <div id="sidebarBackdrop" class="sidebar-backdrop" data-close-sidebar></div>
  <aside id="sidebar" class="sidebar" aria-hidden="true">
    <div class="sidebar-head">
      <h2>&#12513;&#12491;&#12517;&#12540;</h2>
      <button type="button" class="icon-btn" data-close-sidebar aria-label="&#38281;&#12376;&#12427;">&times;</button>
    </div>
    <nav class="sidebar-nav">
      <a class="is-active" href="index.php">UnivaPay&#21462;&#24341;&#19968;&#35239;</a>
      <a href="nyukin/index.php">&#26085;&#21029;&#12480;&#12483;&#12471;&#12517;&#12508;&#12540;&#12489;</a>
      <a href="nyukin/mistake_finder.php">&#20837;&#21147;&#12511;&#12473;&#20505;&#35036;</a>
      <!-- <a href="calc.php">集計</a> -->
    </nav>
    <form class="sidebar-sync" method="post" action="nyukin/sync_customer_payments.php">
      <input type="hidden" name="return_to" value="../index.php<?= !empty($_SERVER['QUERY_STRING']) ? '?' . htmlspecialchars((string)$_SERVER['QUERY_STRING'], ENT_QUOTES, 'UTF-8') : '' ?>">
      <button type="submit" class="side-button">&#33258;&#31038;DB&#12434;&#26356;&#26032;</button>
      <?php if ($lastSyncedLabel !== ''): ?>
        <p>&#26368;&#32066;&#21516;&#26399;: <?= htmlspecialchars($lastSyncedLabel, ENT_QUOTES, 'UTF-8') ?></p>
      <?php endif; ?>
    </form>
    <form class="sidebar-sync" method="post" action="sync_univapay_transactions.php">
      <input type="hidden" name="return_to" value="index.php<?= !empty($_SERVER['QUERY_STRING']) ? '?' . htmlspecialchars((string)$_SERVER['QUERY_STRING'], ENT_QUOTES, 'UTF-8') : '' ?>">
      <input type="hidden" name="start_date" value="<?= htmlspecialchars(date('Y-m-01'), ENT_QUOTES, 'UTF-8') ?>">
      <input type="hidden" name="end_date" value="<?= htmlspecialchars(date('Y-m-d'), ENT_QUOTES, 'UTF-8') ?>">
      <button type="submit" class="side-button">UnivaPay APIを実行</button>
      <p>対象: 今月分</p>
    </form>
    <!-- <section class="theme-switcher" aria-label="表示モード切り替え">
      <span>表示モード</span>
      <button id="themeToggle" class="theme-toggle" type="button" aria-label="ダークモード切り替え" aria-pressed="false">
        <span class="theme-toggle-track">
          <img id="themeIcon" src="img/sol.png" alt="ライトモード">
        </span>
      </button>
    </section> -->
  </aside>
  <header class="site-header">
    <div class="container header-inner">
      <button id="menuButton" class="hamburger-btn" type="button" aria-label="&#12513;&#12491;&#12517;&#12540;&#12434;&#38283;&#12367;" aria-controls="sidebar" aria-expanded="false">
        &#9776;
      </button>
      <div>
        <p class="eyebrow">Database Manager</p>
        <h1>&#20837;&#37329;&#24773;&#22577; | UnivaPay</h1>
      </div>
      <?php if (isset($filters) && is_array($filters)): ?>
        <a
          class="btn btn-primary"
          href="?<?= htmlspecialchars(http_build_query(array_merge($filters, ['export' => 'csv'])), ENT_QUOTES, 'UTF-8'); ?>"
        >CSV&#12456;&#12463;&#12473;&#12509;&#12540;&#12488;</a>
      <?php endif; ?>
    </div>
  </header>
  <?php if ($syncMessage !== '' || $univapaySyncMessage !== '' || $lastSyncedLabel !== ''): ?>
    <div class="container">
      <?php if ($syncMessage !== ''): ?>
        <div class="sync-notice sync-notice-<?= htmlspecialchars($syncStatus, ENT_QUOTES, 'UTF-8') ?>">
          <?= htmlspecialchars($syncMessage, ENT_QUOTES, 'UTF-8') ?>
        </div>
      <?php endif; ?>
      <?php if ($univapaySyncMessage !== ''): ?>
        <div class="sync-notice sync-notice-<?= htmlspecialchars($univapaySyncStatus, ENT_QUOTES, 'UTF-8') ?>">
          <?= htmlspecialchars($univapaySyncMessage, ENT_QUOTES, 'UTF-8') ?>
        </div>
      <?php endif; ?>
      <?php if ($syncMessage === '' && $univapaySyncMessage === '' && $lastSyncedLabel !== ''): ?>
        <div class="sync-meta">&#33258;&#31038;DB &#26368;&#32066;&#21516;&#26399;: <?= htmlspecialchars($lastSyncedLabel, ENT_QUOTES, 'UTF-8') ?></div>
      <?php endif; ?>
    </div>
  <?php endif; ?>
  <main class="container">
