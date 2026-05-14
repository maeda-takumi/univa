<!doctype html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>入金情報 | UnivaPay</title>
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
      <!-- <a href="calc.php">集計</a> -->
    </nav>
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
      <button id="menuButton" class="hamburger-btn" type="button" aria-label="メニューを開く" aria-controls="sidebar" aria-expanded="false">
        ☰
      </button>
      <div>
        <p class="eyebrow">Database Manager</p>
        <h1>入金情報 | UnivaPay</h1>
      </div>
      <button id="importButton" class="btn btn-primary" type="button">データをインポート</button>
    </div>
  </header>
  <main class="container">
