<?php
declare(strict_types=1);
if (!isset($pageTitle)) {
    $pageTitle = 'UnivaPay Webhook Viewer';
}
if (!function_exists('h')) {
    function h(?string $value): string
    {
        return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
    }
}

if (!function_exists('tme')) {
    function tme(string $path): string
    {
        $fullPath = __DIR__ . '/' . ltrim($path, '/');
        return file_exists($fullPath) ? (string)filemtime($fullPath) : (string)time();
    }
}
?>
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?= htmlspecialchars($pageTitle, ENT_QUOTES, 'UTF-8') ?></title>

    <!-- ブラウザタブ用アイコン -->
    <link rel="icon" type="image/png" href="img/icon.png?v=<?= h(tme('img/icon.png')); ?>">

    <!-- iPhoneで「ホーム画面に追加」した時のアイコン -->
    <link rel="apple-touch-icon" href="img/icon.png?v=<?= h(tme('img/icon.png')); ?>">

    <!-- iPhoneホーム画面用の表示名 -->
    <meta name="apple-mobile-web-app-title" content="<?= htmlspecialchars($pageTitle, ENT_QUOTES, 'UTF-8') ?>">

    <!-- PWAっぽく全画面表示したい場合 -->
    <meta name="apple-mobile-web-app-capable" content="yes">

    <!-- ステータスバーの見た目 -->
    <meta name="apple-mobile-web-app-status-bar-style" content="default">

    <link rel="stylesheet" href="style/style.css?v=<?= h(tme('style/style.css')); ?>">
</head>
<body>
<div class="app-shell">
    <header class="site-header">
        <div class="container header-inner">
            <button
                type="button"
                class="menu-toggle"
                id="menuToggle"
                aria-expanded="false"
                aria-controls="sidebarPanel"
                aria-label="サイドバーを開閉"
            >
                <span></span>
                <span></span>
                <span></span>
            </button>
            <div>
                <p class="eyebrow">UNIVA-MIRROR</p>
                <h1 class="site-title"><?= htmlspecialchars($pageTitle, ENT_QUOTES, 'UTF-8') ?></h1>
            </div>
            <div class="header-badge">UnivaPay</div>
        </div>
    </header>
    <main class="container main-content">
