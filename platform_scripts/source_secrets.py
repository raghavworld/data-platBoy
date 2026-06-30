from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from common import STATE_DIR, load_environment


SECRETS_PATH = STATE_DIR / "secrets.json"


def _read_all() -> dict[str, Any]:
    load_environment()
    if not SECRETS_PATH.exists():
        return {}
    return json.loads(SECRETS_PATH.read_text(encoding="utf-8"))


def _write_all(payload: dict[str, Any]) -> None:
    load_environment()
    SECRETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = Path(f"{SECRETS_PATH}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(SECRETS_PATH)


def put_secret(reference: str, payload: dict[str, Any]) -> None:
    secrets = _read_all()
    secrets[reference] = payload
    _write_all(secrets)


def get_secret(reference: str | None) -> dict[str, Any]:
    if not reference:
        return {}
    return _read_all().get(reference, {})


def delete_secret(reference: str | None) -> None:
    if not reference:
        return
    secrets = _read_all()
    if reference in secrets:
        del secrets[reference]
        _write_all(secrets)
