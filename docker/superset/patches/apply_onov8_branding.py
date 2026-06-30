from __future__ import annotations

from pathlib import Path


CSS_TAG = '<link rel="stylesheet" href="/static/assets/onov8/onov8_superset.css">'
JS_TAG = '<script src="/static/assets/onov8/onov8_superset.js"></script>'


def inject_once(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    changed = False
    if CSS_TAG not in text:
        if "</head>" in text:
            text = text.replace("</head>", f"  {CSS_TAG}\n</head>", 1)
            changed = True
        elif "{% block head_css %}" in text:
            text = text.replace("{% block head_css %}", f"{{% block head_css %}}\n  {CSS_TAG}", 1)
            changed = True
    if JS_TAG not in text:
        if "</body>" in text:
            text = text.replace("</body>", f"  {JS_TAG}\n</body>", 1)
            changed = True
        elif "{% block tail_js %}" in text:
            text = text.replace("{% block tail_js %}", f"{{% block tail_js %}}\n  {JS_TAG}", 1)
            changed = True
    if changed:
        path.write_text(text, encoding="utf-8")
    return changed


def main() -> None:
    roots = [Path("/app/superset"), Path("/usr/local/lib")]
    candidates: list[Path] = []
    for root in roots:
        if root.exists():
            candidates.extend(root.rglob("*.html"))
    preferred_names = {
        "basic.html",
        "base.html",
        "baselayout.html",
        "login_db.html",
        "login.html",
    }
    patched = []
    for path in candidates:
        if path.name not in preferred_names:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if "</head>" not in text and "{% block head_css %}" not in text:
            continue
        if inject_once(path):
            patched.append(str(path))
    print("ONOV8 branding template patch applied to:")
    for path in patched or ["no templates matched; config branding still applies"]:
        print(f" - {path}")


if __name__ == "__main__":
    main()
