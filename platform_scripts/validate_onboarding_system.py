#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "dashboard" / "web"
MAIN = WEB / "src" / "main.jsx"
ONBOARDING = WEB / "src" / "onboardingData.js"
GUIDE = WEB / "src" / "userGuideData.js"
STYLES = WEB / "src" / "styles.css"
MAKEFILE = ROOT / "Makefile"
SCREENSHOTS = ROOT / "docs" / "screenshots"

EXPECTED_STEPS = [
    "welcome",
    "sources",
    "raw",
    "bronze",
    "silver",
    "query",
    "bi",
    "governance",
    "monitoring",
    "users",
    "platform-validation",
    "final-readiness",
]
EXPECTED_STATUSES = [
    "Not Started",
    "In Progress",
    "Completed",
    "Warning",
    "Failed",
    "Skipped",
    "Blocked",
]
READINESS_STATES = ["Not Ready", "Partially Configured", "Operational", "Production Ready"]
DICTIONARY_ROUTES = {
    "status-dictionary",
    "button-dictionary",
    "metric-dictionary",
    "dangerous-actions",
    "operational-screenshots",
}


def slugify(value: str) -> str:
    value = value.lower().replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def extract_block(text: str, start: str, end: str) -> str:
    begin = text.find(start)
    if begin == -1:
        return ""
    begin += len(start)
    finish = text.find(end, begin)
    return text[begin:] if finish == -1 else text[begin:finish]


def step_blocks(steps_block: str) -> dict[str, str]:
    matches = list(re.finditer(r'\n\s*\{\s*\n\s*id:\s*"([^"]+)"', steps_block))
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(steps_block)
        blocks[match.group(1)] = steps_block[match.start():end]
    return blocks


def report(condition: bool, message: str, errors: list[str]) -> None:
    if condition:
        print(f"OK: {message}")
    else:
        print(f"FAIL: {message}")
        errors.append(message)


def parse_guide_routes(guide_js: str) -> set[str]:
    sections_block = extract_block(guide_js, "export const GUIDE_SECTIONS = [", "];\n\nexport const GUIDE_SCREENSHOT_PAGES")
    section_pairs = re.findall(r'id:\s*"([^"]+)".*?title:\s*"([^"]+)"', sections_block, re.S)
    topic_block = extract_block(guide_js, "export const GUIDE_TOPICS = [", "];\n\nexport const GUIDE_SECTIONS")
    topic_ids = set(re.findall(r'id:\s*"([^"]+)"', topic_block))
    return {section_id for section_id, _title in section_pairs} | {slugify(title) for _section_id, title in section_pairs} | topic_ids | DICTIONARY_ROUTES


def validate_onboarding_data(onboarding_js: str, guide_routes: set[str], errors: list[str]) -> tuple[list[str], dict[str, str]]:
    steps_block = extract_block(onboarding_js, "export const SETUP_STEPS = [", "];\n\nexport const SETUP_STATUS_SUMMARY")
    blocks = step_blocks(steps_block)
    step_ids = list(blocks)

    report(step_ids == EXPECTED_STEPS, "All 12 onboarding steps exist in the required order", errors)
    report(all(status in onboarding_js for status in EXPECTED_STATUSES), "All checklist statuses are defined", errors)
    report(all(state in onboarding_js for state in READINESS_STATES), "All setup health states are defined", errors)
    report("STEP_ID_BY_ROUTE" in onboarding_js and "lookup[step.route] = step.id" in onboarding_js, "Wizard route lookup is generated from setup steps", errors)

    required_keys = [
        "route:",
        "title:",
        "description:",
        "why:",
        "estimate:",
        "dependencies:",
        "required:",
        "actionLabel:",
        "validateLabel:",
        "learnMore:",
        "troubleshooting:",
        "glossary:",
        "commonMistakes:",
        "recovery:",
        "beginnerHelp:",
        "operatorHelp:",
        "validationChecks:",
    ]
    for step_id in EXPECTED_STEPS:
        block = blocks.get(step_id, "")
        missing = [key for key in required_keys if key not in block]
        report(not missing, f"{step_id} has complete checklist metadata", errors)
        checks = re.findall(r'\{\s*id:\s*"[^"]+".*?label:\s*"[^"]+".*?required:\s*(?:true|false)', block, re.S)
        report(bool(checks), f"{step_id} defines validation checks", errors)

    required_flags = {
        "bi": "required: false",
        "governance": "required: false",
        "monitoring": "required: false",
        "users": "required: false",
    }
    for step_id, token in required_flags.items():
        report(token in blocks.get(step_id, ""), f"{step_id} is marked as optional enhancement", errors)
    for step_id in [item for item in EXPECTED_STEPS if item not in required_flags]:
        report("required: true" in blocks.get(step_id, ""), f"{step_id} is marked required", errors)

    routes = re.findall(r'route:\s*"([^"]+)"', steps_block)
    report(len(routes) == len(set(routes)) == len(EXPECTED_STEPS), "Every onboarding step has a unique deep-link route", errors)

    link_values = re.findall(r'(?:learnMore|troubleshooting):\s*"([^"]+)"', steps_block)
    broken_links = []
    for link in link_values:
        if not link.startswith("/user-guide"):
            broken_links.append(link)
            continue
        slug = link.replace("/user-guide", "", 1).strip("/").split("#", 1)[0]
        if slug and slug not in guide_routes:
            broken_links.append(link)
    report(not broken_links, f"Onboarding Learn More and Troubleshooting links resolve ({len(link_values)} checked)", errors)

    return step_ids, blocks


def validate_react_wiring(main_js: str, step_ids: list[str], errors: list[str]) -> None:
    pages_block = extract_block(main_js, "const pages = [", "];")
    report('"getting-started", label: "Getting Started"' in pages_block, "Getting Started is visible in the sidebar", errors)
    report('"setup-checklist", label: "Setup Checklist"' in pages_block, "Setup Checklist is visible in the sidebar", errors)
    report('"user-guide", label: "User Guide / How to Use"' in pages_block, "Onboarding sidebar items are separate from User Guide", errors)
    report('"setup-wizard", label: "Setup Wizard"' in main_js and "const extraRoutes" in main_js, "Setup Wizard exists as a deep-link route", errors)

    route_tokens = [
        'parts[0] === "setup-wizard"',
        'page: "setup-wizard"',
        "STEP_ID_BY_ROUTE[parts[1]]",
        "function wizardPath",
        "navigateToWizard",
        '"getting-started": GettingStartedPage',
        '"setup-checklist": SetupChecklistPage',
        '"setup-wizard": SetupWizardPage',
    ]
    report(all(token in main_js for token in route_tokens), "Routes /getting-started, /setup-checklist, /setup-wizard, and /setup-wizard/:step are wired", errors)

    persistence_tokens = [
        "ONBOARDING_STORAGE_KEY",
        "localStorage.getItem(ONBOARDING_STORAGE_KEY)",
        "localStorage.setItem(ONBOARDING_STORAGE_KEY",
        "lastSavedAt",
        "completedAt",
        "skippedAt",
        "validationHistory",
        "Reset saved setup progress",
    ]
    report(all(token in main_js for token in persistence_tokens), "Progress persistence, timestamps, validation history, and reset are wired", errors)

    validation_tokens = [
        "async function validateSetupStep",
        "runValidation",
        "validationOutcome",
        "makeCheck",
        "Validate",
        "Validating...",
        "Troubleshooting",
        "Learn More",
        "SetupValidationList",
        "Why is this failing?",
        "Recommended next action",
        "affectedItems",
        "quickActions",
        "Open Logs",
        "SetupValidationHistory",
    ]
    report(all(token in main_js for token in validation_tokens), "Interactive validation, retry, help, and troubleshooting controls are wired", errors)

    missing_step_validation = [step_id for step_id in step_ids if f'step.id === "{step_id}"' not in main_js]
    report(not missing_step_validation, "Every onboarding step has validation/snapshot handling", errors)

    health_tokens = ["function calculateReadiness", "readiness.score", "readiness.state", "missingCritical", "optionalOpen"]
    report(all(token in main_js for token in health_tokens) and all(state in main_js for state in READINESS_STATES), "Setup health scoring and readiness states are wired", errors)

    readiness_tokens = [
        "function buildReadinessValidationChecks",
        "function SetupReadinessReview",
        "Required validations",
        "Optional enhancements",
        "Blocking",
        "Operational severity",
    ]
    report(all(token in main_js for token in readiness_tokens), "Readiness review shows required/optional validation blockers with severity", errors)

    bi_action_tokens = [
        "Superset connectivity",
        "Superset datasets",
        "Superset dashboards",
        "Superset charts",
        "BI validation result",
        "Run BI Validation Again",
        "Rebuild Datasets",
        "Refresh Superset Metadata",
        "Validate BI Visibility",
        "Superset dashboards failed validation because",
    ]
    report(all(token in main_js for token in bi_action_tokens), "BI validation warnings expose exact dataset/dashboard/chart/connectivity failures and fixes", errors)

    mode_tokens = ["mode: \"beginner\"", "onboarding.setMode(\"beginner\")", "onboarding.setMode(\"operator\")", "Operator mode", "operatorHelp", "beginnerHelp"]
    report(all(token in main_js for token in mode_tokens), "Beginner and Operator modes are wired", errors)

    ux_tokens = [
        "SetupArchitectureFlow",
        "setup-score-ring",
        "setup-stepper",
        "setup-checklist",
        "setup-help-details",
        "setup-validation-panel",
        "setup-wizard-nav",
        "Skip optional",
    ]
    report(all(token in main_js for token in ux_tokens), "Premium wizard/checklist UX components are present", errors)


def validate_visual_and_table_fixes(main_js: str, styles_css: str, errors: list[str]) -> None:
    table_tokens = ["function useAutoTablePagination", "rows.length <= 25", "25 per page", ".table-pagination", "useAutoTablePagination(page)"]
    report(all(token in main_js + styles_css for token in table_tokens), "All console tables receive 25-row pagination controls", errors)

    overview_tokens = ["function OverviewRunsSummary", "overview-run-grid", "overview-run-card", "bronzeRuns", "silverRuns", "Last 5 per layer"]
    report(all(token in main_js + styles_css for token in overview_tokens), "Overview latest-runs view is upgraded", errors)

    metadata_tokens = ["metadata-path-flow", ".metadata-path-flow", "overflow-x: auto"]
    report(all(token in main_js + styles_css for token in metadata_tokens), "Governance Metadata Path layout is fixed for overflow", errors)

    setup_style_tokens = [
        ".setup-wizard-layout",
        ".setup-stepper",
        ".setup-check-item",
        ".setup-score-ring",
        ".setup-architecture-flow",
        ".setup-validation-row",
    ]
    report(all(token in styles_css for token in setup_style_tokens), "Onboarding visual styles exist", errors)


def validate_guide_and_makefile(guide_js: str, makefile: str, errors: list[str]) -> None:
    for page_id, screenshot in [("getting-started", "getting-started.svg"), ("setup-checklist", "setup-checklist.svg")]:
        report(f'id: "{page_id}"' in guide_js and f'screenshot: "{screenshot}"' in guide_js, f"User Guide documents {page_id}", errors)
        report((SCREENSHOTS / screenshot).exists(), f"Screenshot placeholder exists for {page_id}", errors)

    make_tokens = ["validate-onboarding:", "python3 scripts/validate_onboarding_system.py", "reset-onboarding:", "python3 scripts/reset_onboarding_progress.py"]
    report(all(token in makefile for token in make_tokens), "Makefile exposes validate-onboarding and reset-onboarding", errors)


def main() -> int:
    errors: list[str] = []
    main_js = MAIN.read_text()
    onboarding_js = ONBOARDING.read_text()
    guide_js = GUIDE.read_text()
    styles_css = STYLES.read_text()
    makefile = MAKEFILE.read_text()

    guide_routes = parse_guide_routes(guide_js)
    step_ids, _blocks = validate_onboarding_data(onboarding_js, guide_routes, errors)
    validate_react_wiring(main_js, step_ids, errors)
    validate_visual_and_table_fixes(main_js, styles_css, errors)
    validate_guide_and_makefile(guide_js, makefile, errors)

    if errors:
        print("\nOnboarding validation failed.")
        return 1
    print("\nOnboarding validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
