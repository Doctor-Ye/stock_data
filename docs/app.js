const normalizedPath = window.location.pathname.replace(/\/?$/, "/");
const target = `${normalizedPath}dashboard-zh.html${window.location.hash}`;

if (!window.location.pathname.endsWith("/dashboard-zh.html")) {
  window.location.replace(target);
}
