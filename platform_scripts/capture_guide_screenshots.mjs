#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(scriptDir, "..");
const webRoot = path.join(projectRoot, "dashboard", "web");
const screenshotDir = path.join(projectRoot, "docs", "screenshots");
const publicScreenshotDir = path.join(webRoot, "public", "docs", "screenshots");
const guideDataUrl = pathToFileURL(path.join(webRoot, "src", "userGuideData.js")).href;
const consoleUrl = process.env.ONOV8_CONSOLE_URL || "http://localhost:5173";
const username = process.env.ONOV8_ADMIN_USERNAME || "admin";
const password = process.env.ONOV8_ADMIN_PASSWORD || "admin";

function ensureDirs() {
  fs.mkdirSync(screenshotDir, { recursive: true });
  fs.mkdirSync(publicScreenshotDir, { recursive: true });
}

function placeholderSvg(title, reason) {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1440" height="900" viewBox="0 0 1440 900">
  <rect width="1440" height="900" fill="#eef2f6"/>
  <rect x="64" y="64" width="1312" height="772" rx="12" fill="#fff" stroke="#c7d3df"/>
  <text x="110" y="150" font-family="Arial, sans-serif" font-size="42" font-weight="700" fill="#17202a">${escapeXml(title)}</text>
  <text x="110" y="215" font-family="Arial, sans-serif" font-size="24" fill="#526171">${escapeXml(reason)}</text>
  <circle cx="116" cy="300" r="20" fill="#1f6f5b"/><text x="110" y="308" font-family="Arial" font-size="20" font-weight="700" fill="#fff">1</text>
  <text x="150" y="308" font-family="Arial, sans-serif" font-size="22" fill="#17202a">Placeholder only. Run make capture-guide-screenshots against the running console.</text>
  </svg>`;
}

function escapeXml(value) {
  return String(value).replace(/[<>&"']/g, (char) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;", "'": "&apos;" })[char]);
}

function writePlaceholder(fileName, title, reason) {
  const svgName = fileName.replace(/\.(png|jpg|jpeg)$/i, ".svg");
  const content = placeholderSvg(title, reason);
  fs.writeFileSync(path.join(screenshotDir, svgName), content);
  fs.writeFileSync(path.join(publicScreenshotDir, svgName), content);
}

function mirror(fileName) {
  fs.copyFileSync(path.join(screenshotDir, fileName), path.join(publicScreenshotDir, fileName));
}

function systemChromePath() {
  const candidates = [
    process.env.PLAYWRIGHT_CHROME_EXECUTABLE,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser"
  ].filter(Boolean);
  return candidates.find((candidate) => fs.existsSync(candidate));
}

async function loadPlaywright() {
  try {
    const requireFromWeb = createRequire(path.join(webRoot, "package.json"));
    const resolved = requireFromWeb.resolve("playwright-core");
    return await import(pathToFileURL(resolved).href);
  } catch (error) {
    return null;
  }
}

async function loginIfNeeded(page) {
  await page.goto(consoleUrl, { waitUntil: "domcontentloaded" });
  const usernameInput = page.locator('input[autocomplete="username"]');
  if (await usernameInput.count()) {
    await usernameInput.first().fill(username);
    await page.locator('input[autocomplete="current-password"]').fill(password);
    await page.getByRole("button", { name: /login/i }).click();
  }
  await page.waitForSelector("aside nav", { timeout: 30000 });
}

async function openSidebarPage(page, label) {
  const navButton = page.locator("aside nav button").filter({ hasText: label }).first();
  await navButton.scrollIntoViewIfNeeded();
  await navButton.click();
  await page.waitForTimeout(850);
  await page.evaluate(() => window.scrollTo(0, 0));
}

async function annotatePage(page) {
  await page.evaluate(() => {
    document.querySelectorAll(".guide-capture-marker, .guide-capture-style").forEach((node) => node.remove());
    document.querySelectorAll(".guide-capture-highlight").forEach((node) => node.classList.remove("guide-capture-highlight"));
    const style = document.createElement("style");
    style.className = "guide-capture-style";
    style.textContent = `
      .guide-capture-highlight {
        box-shadow: 0 0 0 3px rgba(31, 111, 91, 0.75), 0 0 0 8px rgba(31, 111, 91, 0.18) !important;
        border-radius: 8px !important;
      }
      .guide-capture-marker {
        position: absolute;
        z-index: 9999;
        display: grid;
        place-items: center;
        width: 28px;
        height: 28px;
        border-radius: 50%;
        background: #1f6f5b;
        color: #fff;
        border: 3px solid #fff;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.28);
        font: 800 14px/1 Arial, sans-serif;
        pointer-events: none;
      }
    `;
    document.head.appendChild(style);

    const candidates = [
      ".topbar",
      "main .metric-grid",
      "main .tabs, main .filter-grid, main form.form-grid",
      "main .table-wrap",
      "main .operation-actions, main .row-actions, main .top-actions",
      "main .danger-panel, main .empty-state, main .service-list, main .timeline, main .lineage-flow, main .orchestration-flow"
    ];
    const visible = (element) => {
      const rect = element.getBoundingClientRect();
      return rect.width > 20 && rect.height > 20;
    };
    let index = 1;
    candidates.forEach((selector) => {
      const target = Array.from(document.querySelectorAll(selector)).find(visible);
      if (!target || index > 6) return;
      target.classList.add("guide-capture-highlight");
      const rect = target.getBoundingClientRect();
      const marker = document.createElement("div");
      marker.className = "guide-capture-marker";
      marker.textContent = String(index);
      marker.style.left = `${Math.max(8, rect.left + window.scrollX - 12)}px`;
      marker.style.top = `${Math.max(8, rect.top + window.scrollY - 12)}px`;
      document.body.appendChild(marker);
      index += 1;
    });
  });
}

async function clearAnnotations(page) {
  await page.evaluate(() => {
    document.querySelectorAll(".guide-capture-marker, .guide-capture-style").forEach((node) => node.remove());
    document.querySelectorAll(".guide-capture-highlight").forEach((node) => node.classList.remove("guide-capture-highlight"));
  });
}

async function capturePage(page, pageInfo) {
  await openSidebarPage(page, pageInfo.label);
  await annotatePage(page);
  const fileName = `${pageInfo.id}.png`;
  await page.screenshot({ path: path.join(screenshotDir, fileName), fullPage: pageInfo.id !== "user-guide" });
  await clearAnnotations(page);
  mirror(fileName);
  console.log(`captured ${fileName}`);
}

async function captureDialog(page, fileName, openPageLabel, buttonText, tabText = "") {
  await openSidebarPage(page, openPageLabel);
  if (tabText) {
    await page.locator("main .tabs button").filter({ hasText: tabText }).first().click();
    await page.waitForTimeout(400);
  }
  const button = page.getByRole("button", { name: new RegExp(buttonText, "i") }).first();
  await button.scrollIntoViewIfNeeded();
  await button.click();
  await page.waitForSelector(".confirmation-modal", { timeout: 5000 });
  await annotatePage(page);
  await page.screenshot({ path: path.join(screenshotDir, fileName), fullPage: true });
  await clearAnnotations(page);
  await page.getByRole("button", { name: /cancel/i }).last().click();
  mirror(fileName);
  console.log(`captured ${fileName}`);
}

async function captureValidation(page, fileName, pageLabel, tabText, triggerText) {
  await openSidebarPage(page, pageLabel);
  await page.getByRole("button", { name: new RegExp(tabText, "i") }).first().click();
  await page.waitForTimeout(400);
  if (triggerText) {
    const trigger = page.getByRole("button", { name: new RegExp(triggerText, "i") }).first();
    if (await trigger.count()) {
      await trigger.click();
      await page.waitForTimeout(3000);
    }
  }
  await annotatePage(page);
  await page.screenshot({ path: path.join(screenshotDir, fileName), fullPage: true });
  await clearAnnotations(page);
  mirror(fileName);
  console.log(`captured ${fileName}`);
}

async function main() {
  ensureDirs();
  const { GUIDE_SCREENSHOT_PAGES, GUIDE_EXTRA_SCREENSHOTS } = await import(guideDataUrl);
  const playwright = await loadPlaywright();
  const executablePath = systemChromePath();
  if (!playwright || !executablePath) {
    const reason = !playwright ? "playwright-core is not installed." : "No local Chrome/Chromium executable found.";
    [...GUIDE_SCREENSHOT_PAGES, ...GUIDE_EXTRA_SCREENSHOTS.map((item) => ({ id: item.id, label: item.title }))]
      .forEach((item) => writePlaceholder(`${item.id}.png`, item.label, reason));
    console.error(`Screenshot capture could not start: ${reason}`);
    console.error("Install with: cd dashboard/web && npm install --save-dev playwright-core");
    process.exitCode = 2;
    return;
  }

  const chromium = playwright.chromium || playwright.default?.chromium;
  if (!chromium) {
    throw new Error("playwright-core loaded, but chromium launcher was not found");
  }
  const browser = await chromium.launch({ executablePath, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 }, deviceScaleFactor: 1 });
  page.setDefaultTimeout(30000);
  await loginIfNeeded(page);

  for (const pageInfo of GUIDE_SCREENSHOT_PAGES) {
    try {
      await capturePage(page, pageInfo);
    } catch (error) {
      console.error(`failed ${pageInfo.id}: ${error.message}`);
      writePlaceholder(`${pageInfo.id}.png`, pageInfo.label, error.message);
    }
  }

  const extraCaptures = [
    () => captureDialog(page, "dialog-flush-bronze.png", "Maintenance", "Flush Bronze Layer"),
    () => captureDialog(page, "dialog-rebuild-silver.png", "Maintenance", "Rebuild Silver From Bronze"),
    () => captureDialog(page, "dialog-restart-trino.png", "Query Layer", "Restart Trino", "Maintenance"),
    () => captureValidation(page, "validation-query.png", "Query Layer", "Validation", "Validate Query Layer"),
    () => captureValidation(page, "validation-bi.png", "BI / Superset", "BI Validation", "Validate BI Layer")
  ];

  for (const capture of extraCaptures) {
    try {
      await capture();
    } catch (error) {
      const fileName = capture.toString().match(/"([^"]+\.png)"/)?.[1] || `extra-${Date.now()}.png`;
      console.error(`failed ${fileName}: ${error.message}`);
      writePlaceholder(fileName, fileName.replace(".png", ""), error.message);
    }
  }

  await browser.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
