  </main>

  <div id="importModal" class="modal" aria-hidden="true" role="dialog" aria-labelledby="importModalTitle">
    <div class="modal-overlay" data-close-modal></div>
    <section class="modal-panel">
      <header class="modal-header">
        <h2 id="importModalTitle">データインポート</h2>
        <button type="button" class="icon-btn" data-close-modal aria-label="閉じる">×</button>
      </header>
      <div class="modal-content">
        <iframe src="api.php" title="API UI" loading="lazy"></iframe>
      </div>
    </section>
  </div>

  <script src="js/app.js?v=<?= time(); ?>"></script>
</body>
</html>
