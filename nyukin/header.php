<?php
$title = $title ?? json_decode('"\u5165\u91d1\u7ba1\u7406\u30c1\u30a7\u30c3\u30af"');
$active = $active ?? '';
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
<main class="app-shell">
