<?php
require_once __DIR__ . '/reconciliation_data.php';
$title = json_decode('"\u5165\u529b\u30df\u30b9\u5019\u88dc\u30ea\u30b9\u30c8"');
$active = 'mistake';
require __DIR__ . '/header.php';
?>
<section id="mistake-app" class="dashboard-panel" data-app="mistake">
  <script type="application/json" id="app-data"><?= htmlspecialchars(app_json('mistake'), ENT_NOQUOTES, 'UTF-8') ?></script>
  <div class="toolbar toolbar--mistake">
    <label>UnivaPay&#20837;&#37329;&#26376;<select id="monthFilter"></select></label>
    <label>&#20505;&#35036;&#31278;&#21029;<select id="kindFilter"><option value="all">&#12377;&#12409;&#12390;&#12398;&#20505;&#35036;</option></select></label>
    <label>&#30906;&#24230;<select id="confidenceFilter"><option value="all">&#12377;&#12409;&#12390;</option><option value="&#39640;">&#39640;</option><option value="&#20013;">&#20013;</option><option value="&#26410;&#20837;&#21147;">&#26410;&#20837;&#21147;</option><option value="&#20313;&#12426;">&#20313;&#12426;</option></select></label>
    <label>&#26908;&#32034;<input id="search" type="search" placeholder="&#26085;&#20184;&#12539;&#27663;&#21517;&#12539;&#12513;&#12540;&#12523;&#12539;&#34892;&#30058;&#21495;"></label>
  </div>
  <div id="mistakeList" class="candidate-list"></div>
</section>
<?php require __DIR__ . '/footer.php'; ?>
