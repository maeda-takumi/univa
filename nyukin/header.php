<?php
$title = $title ?? json_decode('"\u5165\u91d1\u7ba1\u7406\u30c1\u30a7\u30c3\u30af"');
$active = $active ?? '';
$syncMetaPath = __DIR__ . '/data/sync_meta.json';
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
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title><?= htmlspecialchars($title, ENT_QUOTES, 'UTF-8') ?></title>
  <link rel="stylesheet" href="style/style.css?t=<?= time() ?>">
</head>
<body>
<div id="sidebarBackdrop" class="sidebar-backdrop" data-close-sidebar></div>
<aside id="sidebar" class="sidebar" aria-hidden="true">
  <div class="sidebar-head">
    <h2>&#12513;&#12491;&#12517;&#12540;</h2>
    <button type="button" class="icon-btn" data-close-sidebar aria-label="&#38281;&#12376;&#12427;">&times;</button>
  </div>
  <nav class="sidebar-nav">
    <a href="../index.php">UnivaPay&#21462;&#24341;&#19968;&#35239;</a>
    <a class="<?= $active === 'daily' ? 'is-active' : '' ?>" href="payment_daily_dashboard.php">&#26085;&#21029;&#12480;&#12483;&#12471;&#12517;&#12508;&#12540;&#12489;</a>
    <a class="<?= $active === 'mistake' ? 'is-active' : '' ?>" href="mistake_finder.php">&#20837;&#21147;&#12511;&#12473;&#20505;&#35036;</a>
  </nav>
  <form class="sidebar-sync" method="post" action="sync_customer_payments.php">
    <input type="hidden" name="return_to" value="<?= htmlspecialchars(basename((string)($_SERVER['SCRIPT_NAME'] ?? 'index.php')) . (!empty($_SERVER['QUERY_STRING']) ? '?' . (string)$_SERVER['QUERY_STRING'] : ''), ENT_QUOTES, 'UTF-8') ?>">
    <button type="submit">&#33258;&#31038;DB&#12434;&#26356;&#26032;</button>
    <?php if ($lastSyncedLabel !== ''): ?>
      <p>&#26368;&#32066;&#21516;&#26399;: <?= htmlspecialchars($lastSyncedLabel, ENT_QUOTES, 'UTF-8') ?></p>
    <?php endif; ?>
  </form>
  <form class="sidebar-sync" method="post" action="../sync_univapay_transactions.php">
    <input type="hidden" name="return_to" value="nyukin/<?= htmlspecialchars(basename((string)($_SERVER['SCRIPT_NAME'] ?? 'index.php')) . (!empty($_SERVER['QUERY_STRING']) ? '?' . (string)$_SERVER['QUERY_STRING'] : ''), ENT_QUOTES, 'UTF-8') ?>">
    <input type="hidden" name="start_date" value="<?= htmlspecialchars(date('Y-m-01'), ENT_QUOTES, 'UTF-8') ?>">
    <input type="hidden" name="end_date" value="<?= htmlspecialchars(date('Y-m-d'), ENT_QUOTES, 'UTF-8') ?>">
    <button type="submit">UnivaPay APIを実行</button>
    <p>対象: 今月分</p>
  </form>
</aside>
<header class="site-header">
  <div class="container header-inner">
    <button id="menuButton" class="hamburger-btn" type="button" aria-label="&#12513;&#12491;&#12517;&#12540;&#12434;&#38283;&#12367;" aria-controls="sidebar" aria-expanded="false">
      &#9776;
    </button>
    <div>
      <p class="eyebrow">Database Manager</p>
      <h1><?= htmlspecialchars($title, ENT_QUOTES, 'UTF-8') ?></h1>
      <p class="header-copy">UnivaPay&#12398;SQLite&#12392;&#33258;&#31038;DB&#12398;CSV&#12434;&#35501;&#12415;&#36796;&#12415;&#12289;&#20837;&#37329;&#12474;&#12524;&#12434;&#30906;&#35469;&#12375;&#12414;&#12377;&#12290;</p>
    </div>
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
<main class="app-shell">
