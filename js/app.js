document.addEventListener('DOMContentLoaded', () => {
    const parents = document.querySelectorAll('.tx-parent');

    parents.forEach((button) => {
        button.addEventListener('click', () => {
            const panel = button.nextElementSibling;
            if (!(panel instanceof HTMLElement)) {
                return;
            }

            const isExpanded = button.getAttribute('aria-expanded') === 'true';
            button.setAttribute('aria-expanded', String(!isExpanded));
            panel.hidden = isExpanded;
        });
    });
});