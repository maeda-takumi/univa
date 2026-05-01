<!doctype html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DBファイル一覧 | UniVa</title>
  <link rel="stylesheet" href="style/style.css?v=<?= time(); ?>">
</head>
<body>
  <div id="sidebarBackdrop" class="sidebar-backdrop" data-close-sidebar></div>
  <aside id="sidebar" class="sidebar" aria-hidden="true">
    <div class="sidebar-head">
      <h2>メニュー</h2>
      <button type="button" class="icon-btn" data-close-sidebar aria-label="閉じる">×</button>
    </div>
    <nav class="sidebar-nav">
      <a href="index.php">取引一覧</a>
      <a href="calc.php">集計</a>
    </nav>
  </aside>
  <header class="site-header">
    <div class="container header-inner">
      <button id="menuButton" class="hamburger-btn" type="button" aria-label="メニューを開く" aria-controls="sidebar" aria-expanded="false">
        ☰
      </button>
      <div>
        <p class="eyebrow">Database Manager</p>
        <h1>DBファイル一覧</h1>
      </div>
      <button id="importButton" class="btn btn-primary" type="button">データをインポート</button>
    </div>
  </header>
  <main class="container">
