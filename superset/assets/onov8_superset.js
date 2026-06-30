(function () {
  "use strict";

  var APP_NAME = "ONOV8 Report System";
  var replacements = [
    [/\bApache Superset\b/g, APP_NAME],
    [/\bSuperset\b/g, APP_NAME],
    [/\bSQL Lab\b/g, "Query Studio"],
    [/\bDashboards\b/g, "Reports"],
    [/\bDashboard\b/g, "Report"],
    [/\bCharts\b/g, "Visualizations"],
    [/\bChart\b/g, "Visualization"],
    [/\bDatasets\b/g, "Data Sources"],
    [/\bDataset\b/g, "Data Source"]
  ];

  function rewriteText(value) {
    var next = value;
    replacements.forEach(function (pair) {
      next = next.replace(pair[0], pair[1]);
    });
    return next;
  }

  function shouldSkip(node) {
    var parent = node && node.parentElement;
    if (!parent) return true;
    return /^(SCRIPT|STYLE|NOSCRIPT|TEXTAREA|INPUT|CODE|PRE)$/.test(parent.tagName);
  }

  function rewriteDom(root) {
    if (!root || !document.body) return;
    document.title = rewriteText(document.title || APP_NAME) || APP_NAME;
    var walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    var node;
    var updates = [];
    while ((node = walker.nextNode())) {
      if (shouldSkip(node)) continue;
      var next = rewriteText(node.nodeValue || "");
      if (next !== node.nodeValue) updates.push([node, next]);
    }
    updates.forEach(function (item) {
      item[0].nodeValue = item[1];
    });
    document.querySelectorAll("[title],[aria-label],[placeholder]").forEach(function (element) {
      ["title", "aria-label", "placeholder"].forEach(function (attribute) {
        var value = element.getAttribute(attribute);
        if (value) element.setAttribute(attribute, rewriteText(value));
      });
    });
  }

  function injectLoginBrand() {
    if (document.querySelector(".onov8-login-brand")) return;
    var password = document.querySelector('input[type="password"]');
    if (!password) return;
    var form = password.closest("form");
    if (!form) return;
    var brand = document.createElement("div");
    brand.className = "onov8-login-brand";
    brand.innerHTML = '<img src="/static/assets/onov8/onov8-logo.svg" alt="ONOV8 Report System" />' +
      "<strong>ONOV8 Report System</strong>" +
      "<span>Secure enterprise reporting workspace</span>";
    form.insertBefore(brand, form.firstChild);
  }

  function run() {
    rewriteDom(document.body);
    injectLoginBrand();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }

  var observer = new MutationObserver(function () {
    window.requestAnimationFrame(run);
  });
  observer.observe(document.documentElement, { childList: true, subtree: true });
}());
