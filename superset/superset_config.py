from __future__ import annotations

import os

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DATABASE_URI"]
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True

APP_NAME = "ONOV8 Report System"
APP_ICON = "/static/assets/onov8/onov8-logo.svg"
APP_ICON_WIDTH = 170
LOGO_TARGET_PATH = "/superset/welcome/"
LOGO_TOOLTIP = "ONOV8 Report System"
LOGO_RIGHT_TEXT = "Report System"
FAVICONS = [{"href": "/static/assets/onov8/favicon.svg"}]
WELCOME_PAGE_TITLE = "ONOV8 Report System"

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}

THEME_OVERRIDES = {
    "colors": {
        "primary": {"base": "#38bdf8", "dark1": "#0ea5e9", "light1": "#7dd3fc"},
        "secondary": {"base": "#22c55e", "dark1": "#16a34a", "light1": "#86efac"},
        "grayscale": {
            "base": "#94a3b8",
            "dark1": "#64748b",
            "dark2": "#334155",
            "dark3": "#1e293b",
            "dark4": "#0f172a",
            "dark5": "#070b16",
            "light1": "#cbd5e1",
            "light2": "#e2e8f0",
            "light3": "#f8fafc",
        },
    },
    "borderRadius": 6,
    "typography": {
        "families": {
            "sansSerif": "'Inter', 'Segoe UI', Arial, sans-serif",
            "monospace": "'JetBrains Mono', 'SFMono-Regular', Consolas, monospace",
        },
    },
}
