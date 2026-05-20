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
  <main class="container">
