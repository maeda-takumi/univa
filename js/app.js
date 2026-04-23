document.addEventListener('DOMContentLoaded', () => {
    const appShell = document.querySelector('.app-shell');
    const menuToggle = document.getElementById('menuToggle');
    const sidebarOverlay = document.getElementById('sidebarOverlay');

    if (!appShell || !menuToggle || !sidebarOverlay) {
        return;
    }

    const syncExpanded = () => {
        const isOpen = appShell.classList.contains('sidebar-open');
        menuToggle.setAttribute('aria-expanded', String(isOpen));
    };

    const toggleSidebar = () => {
        appShell.classList.toggle('sidebar-open');
        syncExpanded();
    };

    const closeSidebar = () => {
        appShell.classList.remove('sidebar-open');
        syncExpanded();
    };

    menuToggle.addEventListener('click', toggleSidebar);
    sidebarOverlay.addEventListener('click', closeSidebar);

    syncExpanded();
});