<?php
require_once __DIR__ . '/reconciliation_data.php';
$title = json_decode('"\u65e5\u5225\u5165\u91d1\u30c0\u30c3\u30b7\u30e5\u30dc\u30fc\u30c9"');
$active = 'daily';
require __DIR__ . '/header.php';
?>
<section id="daily-app" class="dashboard-panel" data-app="daily">
  <script type="application/json" id="app-data"><?= htmlspecialchars(app_json('daily'), ENT_NOQUOTES, 'UTF-8') ?></script>
  <div class="toolbar">
    <label>&#34920;&#31034;&#26376;<select id="period"></select></label>
    <label>&#26085;&#20184;&#26908;&#32034;<input id="dateSearch" type="search" placeholder="&#20363;: 2026-05"></label>
    <label>&#24046;&#38989;<select id="diffFilter"><option value="all">&#12377;&#12409;&#12390;</option><option value="nonzero">&#24046;&#38989;&#12354;&#12426;</option><option value="positive">UnivaPay&#12364;&#22810;&#12356;</option><option value="negative">&#33258;&#31038;DB&#12364;&#22810;&#12356;</option></select></label>
  </div>
  <div class="metrics">
    <div class="metric"><span>&#33258;&#31038;DB</span><strong id="dbTotal">0&#20870;</strong><small id="dbCount">0&#20214;</small></div>
    <div class="metric"><span>UnivaPay</span><strong id="txTotal">0&#20870;</strong><small id="txCount">0&#20214;</small></div>
    <div class="metric"><span>&#24046;&#38989;</span><strong id="diffTotal">0&#20870;</strong><small>UnivaPay - &#33258;&#31038;DB</small></div>
    <div class="metric"><span>&#24046;&#38989;&#12364;&#12354;&#12427;&#26085;</span><strong id="diffDays">0&#26085;</strong><small id="rangeText"></small></div>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>&#20837;&#37329;&#26085;</th><th class="num">&#33258;&#31038;DB</th><th class="num">DB&#20214;&#25968;</th><th class="num">UnivaPay</th><th class="num">Univa&#20214;&#25968;</th><th class="num">&#24046;&#38989;</th><th>&#25805;&#20316;</th></tr></thead>
      <tbody id="dailyBody"></tbody>
    </table>
  </div>
  <dialog id="detailDialog" class="modal">
    <div class="modal-head"><h2 id="detailTitle"></h2><button id="detailClose" type="button">&times;</button></div>
    <div class="modal-summary" id="detailSummary"></div>
    <h3>&#32016;&#12389;&#12369;&#34920;&#31034;</h3>
    <div class="table-wrap"><table><thead><tr><th>&#29366;&#24907;</th><th>UnivaPay</th><th>&#33258;&#31038;DB</th></tr></thead><tbody id="detailLinkBody"></tbody></table></div>
    <div class="detail-grid">
      <section><h3>UnivaPay&#26126;&#32048;</h3><div class="table-wrap"><table><thead><tr><th>&#34892;</th><th>&#27663;&#21517;</th><th>&#12513;&#12540;&#12523;</th><th class="num">&#37329;&#38989;</th><th>&#26041;&#27861;</th></tr></thead><tbody id="detailTxBody"></tbody></table></div></section>
      <section><h3>&#33258;&#31038;DB&#26126;&#32048;</h3><div class="table-wrap"><table><thead><tr><th>&#34892;</th><th>&#27663;&#21517;</th><th>&#12513;&#12540;&#12523;</th><th class="num">&#37329;&#38989;</th><th>&#20837;&#37329;&#20808;</th></tr></thead><tbody id="detailDbBody"></tbody></table></div></section>
    </div>
  </dialog>
</section>
<?php require __DIR__ . '/footer.php'; ?>
