(function () {
  function ensureIndicator() {
    if (document.querySelector(".page-loading-indicator")) return;

    const wrapper = document.createElement("div");
    wrapper.className = "page-loading-indicator";
    wrapper.innerHTML =
      '<div class="page-loading-bar" aria-hidden="true"></div>' +
      '<div class="page-loading-pill" role="status" aria-live="polite">Loading page...</div>';
    document.body.appendChild(wrapper);
  }

  function startLoading() {
    ensureIndicator();
    document.body.classList.add("is-page-loading");
  }

  function stopLoading() {
    document.body.classList.remove("is-page-loading");
  }

  window.addEventListener("DOMContentLoaded", ensureIndicator);
  window.addEventListener("load", stopLoading);
  window.addEventListener("pageshow", stopLoading);

  document.addEventListener("click", (event) => {
    const link = event.target.closest("a[href]");
    if (!link) return;
    if (link.target && link.target !== "_self") return;
    if (link.hasAttribute("download")) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

    const href = link.getAttribute("href") || "";
    if (!href || href.startsWith("#") || href.startsWith("mailto:") || href.startsWith("tel:")) return;

    try {
      const url = new URL(href, window.location.href);
      if (url.origin !== window.location.origin) return;
      if (url.pathname === window.location.pathname && url.hash) return;
    } catch (_) {
      return;
    }

    startLoading();
  });
})();
