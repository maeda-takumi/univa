</main>
<script>
(() => {
  const sidebar = document.getElementById('sidebar');
  const sidebarBackdrop = document.getElementById('sidebarBackdrop');
  const menuButton = document.getElementById('menuButton');
  const closeButtons = document.querySelectorAll('[data-close-sidebar]');

  const openSidebar = () => {
    if (!sidebar || !sidebarBackdrop || !menuButton) return;
    sidebar.classList.add('is-open');
    sidebarBackdrop.classList.add('is-open');
    sidebar.setAttribute('aria-hidden', 'false');
    menuButton.setAttribute('aria-expanded', 'true');
  };

  const closeSidebar = () => {
    if (!sidebar || !sidebarBackdrop || !menuButton) return;
    sidebar.classList.remove('is-open');
    sidebarBackdrop.classList.remove('is-open');
    sidebar.setAttribute('aria-hidden', 'true');
    menuButton.setAttribute('aria-expanded', 'false');
  };

  if (menuButton) menuButton.addEventListener('click', openSidebar);
  closeButtons.forEach((button) => button.addEventListener('click', closeSidebar));
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeSidebar();
  });
})();
</script>
<script src="js/app.js?t=<?= time() ?>"></script>
</body>
</html>
