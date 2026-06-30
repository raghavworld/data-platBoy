#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "dashboard" / "web"
MAIN = WEB / "src" / "main.jsx"
GUIDE = WEB / "src" / "userGuideData.js"
README = ROOT / "README.md"
SCREENSHOTS = ROOT / "docs" / "screenshots"


def slugify(value: str) -> str:
    value = value.lower().replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def fail(message: str, errors: list[str]) -> None:
    errors.append(message)
    print(f"FAIL: {message}")


def ok(message: str) -> None:
    print(f"OK: {message}")


def extract_block(text: str, start: str, end: str) -> str:
    pattern = re.compile(re.escape(start) + r"(.*?)" + re.escape(end), re.S)
    match = pattern.search(text)
    return match.group(1) if match else ""


def main() -> int:
    errors: list[str] = []
    main_js = MAIN.read_text()
    guide_js = GUIDE.read_text()
    readme = README.read_text()

    pages_block = extract_block(main_js, "const pages = [", "];")
    page_entries = re.findall(r'\{\s*id:\s*"([^"]+)",\s*label:\s*"([^"]+)"', pages_block)
    page_ids = [page_id for page_id, _label in page_entries]
    page_slugs = {page_id: slugify(label if page_id != "user-guide" else "user-guide") for page_id, label in page_entries}
    if "user-guide" in page_ids and '"user-guide": UserGuidePage' in main_js and all(token in main_js for token in ["routeFromLocation", "guidePath", "pagePath", "guideTargetFromSlug"]):
        ok("User Guide sidebar item, component route, path routing, and guide deep-link routing exist")
    else:
        fail("User Guide route/sidebar wiring is incomplete", errors)

    guide_sections_block = extract_block(guide_js, "export const GUIDE_SECTIONS = [", "];\n\nexport const GUIDE_SCREENSHOT_PAGES")
    guide_entries = re.findall(r'id:\s*"([^"]+)".*?title:\s*"([^"]+)".*?category:\s*"([^"]+)"', guide_sections_block, re.S)
    guide_ids = {guide_id for guide_id, _title, _category in guide_entries}
    guide_slugs = {guide_id: slugify(title) for guide_id, title, _category in guide_entries}
    missing_sections = [page_id for page_id in page_ids if page_id not in guide_ids]
    if missing_sections:
        fail(f"Missing guide sections for sidebar pages: {', '.join(missing_sections)}", errors)
    else:
        ok("Every sidebar page has a guide section")

    required_inventory_keys = [
        "tabs",
        "controls",
        "buttons",
        "badges",
        "dialogs",
        "charts",
        "warnings",
        "emptyStates",
        "loadingStates",
        "pagination",
    ]
    for page_id in page_ids:
        section_start = guide_sections_block.find(f'id: "{page_id}"')
        if section_start == -1:
            section_text = ""
        else:
            section_end = guide_sections_block.find("\n  {\n    id:", section_start + 1)
            section_text = guide_sections_block[section_start: section_end if section_end != -1 else len(guide_sections_block)]
        for key in required_inventory_keys:
            if f"{key}:" not in section_text:
                fail(f"{page_id} guide section missing UI inventory key `{key}`", errors)
        for required in ["purpose:", "whenToUse:", "safeDanger:", "normal:", "warning:", "wrong:", "related:", "annotations:"]:
            if required not in section_text:
                fail(f"{page_id} guide section missing `{required.rstrip(':')}`", errors)

    required_route_tokens = [
        "/user-guide",
        "sectionSlug(section)",
        "navigateToGuide(sectionSlug(section))",
        "navigateToPage(section.id)",
        "Read guide",
        "Open page",
        "Open this page",
        "Previous guide page",
        "Next guide page",
        "Back to User Guide home",
    ]
    missing_route_tokens = [token for token in required_route_tokens if token not in main_js]
    if missing_route_tokens:
        fail(f"Guide navigation UI missing tokens: {', '.join(missing_route_tokens)}", errors)
    else:
        ok("Guide Read/Open/Previous/Next/Home navigation controls are wired")

    route_terms = set(re.findall(r'slug:\s*"([^"]+)"', main_js))
    topic_block = extract_block(guide_js, "export const GUIDE_TOPICS = [", "];\n\nexport const GUIDE_SECTIONS")
    topic_ids = set(re.findall(r'id:\s*"([^"]+)"', topic_block))
    dictionary_slugs = {"status-dictionary", "button-dictionary", "metric-dictionary", "dangerous-actions", "operational-screenshots"}
    valid_guide_routes = set(guide_slugs.values()) | topic_ids | dictionary_slugs
    unresolved_quick_routes = sorted(term for term in route_terms if term not in valid_guide_routes)
    if unresolved_quick_routes:
        fail(f"Quick-start or dictionary route slugs do not resolve: {', '.join(unresolved_quick_routes)}", errors)
    else:
        ok("Quick-start guide routes resolve")

    if all(token in main_js for token in ["function pageSlug", "function pagePath", "pageSlug(item)", "navigateToPage(section.id)", "item.id === parts[0] || pageSlug(item) === parts[0]"]):
        ok("Open page route generation matches real sidebar pages")
    else:
        fail("Open page route generation does not map sidebar pages and friendly page slugs", errors)

    if SCREENSHOTS.is_dir():
        ok("docs/screenshots folder exists")
    else:
        fail("docs/screenshots folder is missing", errors)

    missing_screenshots = []
    for page_id in page_ids:
        candidates = [
            SCREENSHOTS / f"{page_id}.png",
            SCREENSHOTS / f"{page_id}.jpg",
            SCREENSHOTS / f"{page_id}.jpeg",
            SCREENSHOTS / f"{page_id}.svg",
        ]
        if not any(path.exists() for path in candidates):
            missing_screenshots.append(page_id)
    if missing_screenshots:
        fail(f"Missing screenshot or placeholder for pages: {', '.join(missing_screenshots)}", errors)
    else:
        ok("Every sidebar page has a screenshot or placeholder")

    extra_files = re.findall(r'file:\s*"([^"]+)"', guide_js)
    missing_extra = [file_name for file_name in extra_files if not (SCREENSHOTS / file_name).exists() and not (SCREENSHOTS / file_name.replace(".png", ".svg")).exists()]
    if missing_extra:
        fail(f"Missing extra operational screenshots/placeholders: {', '.join(missing_extra)}", errors)
    else:
        ok("Operational dialog and validation screenshots exist")

    dictionary_checks = {
        "STATUS_DICTIONARY": ["healthy", "degraded", "failed", "unknown", "success", "no_new_data", "duplicate_batch_skipped", "pending", "processing", "skipped", "warning", "validation_failed", "fallback", "unavailable"],
        "BUTTON_DICTIONARY": ["Run Raw Ingestion", "Run Bronze", "Run Silver", "Validate", "Refresh", "Flush", "Rebuild", "Soft Reset", "Replay", "Restart Service", "Clear Logs", "Export Backup", "Restore", "Open Superset", "Open OpenMetadata"],
        "METRIC_DICTIONARY": ["row counts", "file counts", "storage size", "schema changes", "failed files", "pending files", "slow queries", "failed queries", "validation status", "service health", "uptime", "memory", "CPU", "restart count", "alerts", "audit events"],
        "DANGEROUS_ACTIONS": ["Flush RAW", "Flush Bronze", "Flush Silver", "Rebuild Bronze", "Rebuild Silver", "Full Pipeline Rebuild", "Clear All Logs", "Restart Service", "Restore Backup"],
    }
    for dictionary_name, terms in dictionary_checks.items():
        if f"export const {dictionary_name}" not in guide_js:
            fail(f"{dictionary_name} is missing", errors)
            continue
        missing_terms = [term for term in terms if term not in guide_js]
        if missing_terms:
            fail(f"{dictionary_name} missing terms: {', '.join(missing_terms)}", errors)
        else:
            ok(f"{dictionary_name} includes required terms")

    if all(token in main_js for token in ["`${dictionary.slug}-${slugify(row.term)}`", "anchor:", "navigateToGuide(result.slug, result.anchor || \"\")"]):
        ok("Glossary anchors and exact search-result navigation are wired")
    else:
        fail("Glossary anchors or exact search-result navigation are incomplete", errors)

    if all(token in main_js for token in ["guide-search", "searchTerm", "buildGuideSearchIndex", "Category", "Beginner/User", "Operator/Admin", "guide-mobile-selector"]):
        ok("Guide search, category filters, mode switch, and mobile selector are wired")
    else:
        fail("Guide search/filter/mode/mobile wiring is incomplete", errors)

    search_block = extract_block(main_js, "function buildGuideSearchIndex()", "function targetLabel")
    missing_search_pages = [page_id for page_id in page_ids if page_id not in guide_js or "GUIDE_SECTIONS.flatMap" not in search_block]
    if missing_search_pages:
        fail(f"Search index may not include sidebar pages: {', '.join(missing_search_pages)}", errors)
    else:
        ok("Search index includes all guide/sidebar page sections")

    static_hrefs = re.findall(r'href=["\']([^"\']+)["\']', main_js)
    broken_hrefs = [href for href in static_hrefs if href.startswith("#") and href[1:] not in main_js]
    if broken_hrefs:
        fail(f"Broken static hrefs found: {', '.join(broken_hrefs)}", errors)
    else:
        ok("No broken static hrefs found")

    if all(token in main_js for token in ["lookupButtonHelp", "useMetricHelpTooltips", "metric-help-icon", "help-dot"]):
        ok("Console-wide button and metric help affordances are wired")
    else:
        fail("Console-wide help affordances are incomplete", errors)

    if "User Guide / How to Use" in readme and "validate-user-guide" in readme and "capture-guide-screenshots" in readme:
        ok("README mentions the in-app guide and validation/capture commands")
    else:
        fail("README does not mention the in-app guide and commands", errors)

    if re.search(r"TODO|__PLACEHOLDER_DOC__|GENERATED PLACEHOLDER", guide_js):
        fail("Guide contains unresolved placeholder documentation markers", errors)

    if errors:
        print("\nUser guide validation failed.")
        return 1
    print("\nUser guide validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
