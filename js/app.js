(() => {
  const modal = document.getElementById('importModal');
  const openButton = document.getElementById('importButton');
  const closeButtons = document.querySelectorAll('[data-close-modal]');
  const sidebar = document.getElementById('sidebar');
  const sidebarBackdrop = document.getElementById('sidebarBackdrop');
  const menuButton = document.getElementById('menuButton');
  const sidebarCloseButtons = document.querySelectorAll('[data-close-sidebar]');
  const importButtonSidebar = document.getElementById('importButtonSidebar');
  const themeToggle = document.getElementById('themeToggle');
  const themeIcon = document.getElementById('themeIcon');

  const applyTheme = (mode) => {
    const isDark = mode === 'dark';
    document.body.classList.toggle('dark-mode', isDark);
    if (themeToggle) {
      themeToggle.setAttribute('aria-pressed', isDark ? 'true' : 'false');
    }
    if (themeIcon) {
      themeIcon.src = isDark ? 'img/moon.png' : 'img/sol.png';
      themeIcon.alt = isDark ? 'ダークモード' : 'ライトモード';
    }
  };

  const storedTheme = localStorage.getItem('themeMode');
  applyTheme(storedTheme === 'dark' ? 'dark' : 'light');

  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      const nextMode = document.body.classList.contains('dark-mode') ? 'light' : 'dark';
      applyTheme(nextMode);
      localStorage.setItem('themeMode', nextMode);
    });
  }


  const openModal = () => {
    if (!modal) return;
    modal.classList.add('is-open');
    modal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';
  };

  const closeModal = () => {
    if (!modal) return;
    modal.classList.remove('is-open');
    modal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
  };

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

  if (openButton) {
    openButton.addEventListener('click', openModal);
  }
  if (importButtonSidebar) {
    importButtonSidebar.addEventListener('click', () => {
      openModal();
      closeSidebar();
    });
  }
  if (menuButton) {
    menuButton.addEventListener('click', openSidebar);
  }
  sidebarCloseButtons.forEach((button) => button.addEventListener('click', closeSidebar));
  closeButtons.forEach((button) => button.addEventListener('click', closeModal));

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeModal();
    if (event.key === 'Escape') closeSidebar();
  });
})();
