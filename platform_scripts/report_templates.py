from __future__ import annotations

import difflib
import copy
import json
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import DictCursor, Json

from bi_layer import (
    SupersetClient,
    dynamic_dashboard_spec_for_dataset,
    ensure_superset_database,
    ensure_superset_dashboards,
    ensure_superset_dataset,
    superset_url,
    upsert_bi_dataset,
)
from common import json_safe, load_environment, safe_identifier, safe_json_dumps
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db, new_id
from query_layer import (
    QUERY_PII_HASH_VERSION,
    is_pii_column,
    missing_safe_views,
    pii_safe_columns,
    quote_identifier,
    safe_views_detail,
    safe_views_metadata,
    source_scope_from_table,
    source_table_for_safe_view,
    table_columns,
    table_row_count,
    trino_query,
)


load_environment()


EXAMPLE_TEMPLATE_FIELDS = [
    "Source Type",
    "Submitted by",
    "Submitted By Email",
    "Channel Partner Name",
    "formation_number",
    "Created Date",
    "approvedCompanyName",
    "approvedNameArabic",
    "Formation Type",
    "Stakeholder Type",
    "Stakeholder Name",
    "Email Address",
    "Designation",
    "Date of Birth",
    "Nationality",
    "Stakeholder Status",
    "Company Status",
    "Visa Allocated",
    "Visa Used",
]

CHILD_TABLE_MARKERS = ("stakeholder", "visa", "license", "activity", "activities", "request", "response")
SENSITIVE_MARKERS = ("email", "phone", "mobile", "passport", "emirates", "dob", "date_of_birth", "dateofbirth", "name")
STOP_WORDS = {"a", "an", "and", "by", "for", "from", "in", "of", "the", "to"}
NOISE_WORDS = {"name", "value", "detail", "details", "info", "information"}
COMMON_SUFFIX_TOKENS = {"id", "no", "num", "number", "code", "ref", "reference"}
ABBREVIATIONS = {
    "app": "application",
    "apps": "application",
    "no": "number",
    "num": "number",
    "nbr": "number",
    "ref": "reference",
    "org": "organization",
    "dept": "department",
    "biz": "business",
    "cust": "customer",
    "svc": "service",
    "dt": "date",
    "dob": "datebirth",
}
TOKEN_SYNONYMS: dict[str, tuple[str, ...]] = {
    "application": ("application", "app", "request", "process", "case"),
    "app": ("application", "app", "request"),
    "number": ("number", "no", "num", "id", "code", "ref", "reference"),
    "no": ("number", "no", "num", "id"),
    "company": ("company", "business", "organization", "organisation", "account", "customer"),
    "organization": ("organization", "organisation", "company", "business"),
    "organisation": ("organization", "organisation", "company", "business"),
    "team": ("team", "department", "group", "unit", "queue"),
    "full": ("full", "complete"),
    "name": ("name", "title", "label"),
    "assigned": ("assigned", "assignment", "allocated", "owner"),
    "date": ("date", "time", "timestamp", "at", "on"),
    "start": ("start", "started", "begin", "from"),
    "started": ("start", "started", "begin"),
    "end": ("end", "ended", "finish", "finished", "complete", "completed", "to"),
    "waiting": ("waiting", "wait", "queue", "pending", "delay"),
    "time": ("time", "duration", "elapsed", "period", "sla"),
    "service": ("service", "process", "workflow", "request"),
    "type": ("type", "kind", "category", "classification"),
    "status": ("status", "state", "stage", "phase"),
    "email": ("email", "mail"),
    "submitted": ("submitted", "submitter", "created", "creator"),
    "created": ("created", "submitted", "inserted"),
}
SEMANTIC_ALIASES: dict[str, tuple[str, ...]] = {
    "application number": (
        "application_number",
        "application_no",
        "applicationnumber",
        "application_id",
        "formation_number",
        "formationnumber",
        "request_number",
        "process_request_number",
        "case_number",
        "reference_number",
    ),
    "company name": ("company_name", "company", "approved_company_name", "approvedcompanyname", "company_approved_name", "business_name", "organization_name", "organisation_name", "account_name"),
    "team": ("team", "team_name", "assigned_team", "department", "department_name"),
    "full name": ("full_name", "fullname", "stakeholder_name", "applicant_name", "employee_name", "customer_name", "user_name", "name"),
    "assigned date": ("assigned_date", "assignment_date", "allocated_at", "allocation_date", "assigned_at", "created_at", "submitted_at"),
    "start date": ("start_date", "started_at", "start_time", "created_at", "from_date"),
    "end date": ("end_date", "ended_at", "completed_at", "completedon", "closed_at", "finish_date", "to_date"),
    "waiting time": ("waiting_time", "actualtime", "allottedtime", "queue_time", "pending_duration", "wait_duration"),
    "time taken": ("time_taken", "duration", "processing_time", "elapsed_time", "turnaround_time"),
    "service type name": ("service_type_name", "service_name", "process_name", "service_type"),
    "application status": ("application_status", "status", "ds_status", "request_status", "process_status"),
    "source type": ("source_type", "source", "channel", "source_name", "source_system"),
    "submitted by": ("submitted_by", "submitter", "created_by", "user_name", "username", "submittedby"),
    "submitted by email": ("submitted_by_email", "submitter_email", "user_email", "email_address", "submittedbyemail"),
    "channel partner name": ("channel_partner_name", "partner_name", "channelpartnername"),
    "formation_number": ("formation_number", "formationnumber", "license_number", "registration_number"),
    "created date": ("created_date", "created_at", "createdat", "creation_date", "inserted_at"),
    "approvedcompanyname": ("approved_company_name", "company_approved_name", "approvedcompanyname", "company_name"),
    "approvednamearabic": ("approved_name_arabic", "approvednamearabic", "arabic_name", "company_name_arabic"),
    "formation type": ("formation_type", "formationtype", "company_type", "license_type"),
    "stakeholder type": ("stakeholder_type", "stakeholdertype", "shareholder_type", "person_type"),
    "stakeholder name": ("stakeholder_name", "stakeholdername", "shareholder_name", "person_name", "name"),
    "email address": ("email_address", "email", "stakeholder_email", "user_email"),
    "designation": ("designation", "job_title", "position", "role"),
    "date of birth": ("date_of_birth", "dateofbirth", "dob", "birth_date"),
    "nationality": ("nationality", "country", "citizenship"),
    "stakeholder status": ("stakeholder_status", "stakeholderstatus", "person_status", "status"),
    "company status": ("company_status", "companystatus", "license_status", "status"),
    "visa allocated": ("visa_allocated", "visaallocated", "allocated_visas", "visa_quota"),
    "visa used": ("visa_used", "visaused", "used_visas", "consumed_visas"),
}

SAFE_VIEW_METADATA_CACHE_SECONDS = 300
_SAFE_VIEW_METADATA_CACHE: dict[str, Any] = {"expires_at": 0.0, "metadata": None}
_METADATA_LAYER_CACHE: dict[str, Any] = {
    "expires_at": 0.0,
    "loaded_at": None,
    "metadata": None,
}

AUTO_SELECT_CONFIDENCE = 0.85
NEEDS_REVIEW_CONFIDENCE = 0.70
CANDIDATE_MIN_CONFIDENCE = 0.50
EXACT_MATCH_TYPES = {"exact", "normalized"}
ALIAS_MATCH_TYPES = {"business_alias", "historical"}
STRONG_MATCH_TYPES = EXACT_MATCH_TYPES | ALIAS_MATCH_TYPES | {"acronym", "partial_word", "token"}
_HISTORICAL_ALIAS_CACHE: dict[str, set[str]] = {}

REPORT_STATUS_DRAFT = "draft"
REPORT_STATUS_MAPPING = "mapping_in_progress"
REPORT_STATUS_READY = "ready"
REPORT_STATUS_GENERATED = "generated"
REPORT_STATUS_FAILED = "failed"
REPORT_STATUS_NEEDS_REVIEW = "needs_review"
REPORT_STATUS_NEEDS_REGENERATION = "needs_regeneration"
REPORT_BUILDER_AUTO_FIX_MESSAGE = "Auto-fixed to one safe view for Report Builder v1."
REPORT_AUTO_FIX_MIN_CONFIDENCE = 0.66
SMART_MATCH_MAX_PREFILTERED_CANDIDATES = 120
REPORT_GENERATION_PROGRESS_WEIGHTS = {
    "mapping_validation": 12,
    "schema_discovery": 24,
    "smart_match_processing": 30,
    "sql_generation": 42,
    "report_save": 58,
    "dataset_registration": 72,
    "superset_api_calls": 90,
    "metadata_updates": 100,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def init_report_template_db() -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS report_templates (
                    id UUID PRIMARY KEY,
                    template_name TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'draft',
                    generation_status TEXT NOT NULL DEFAULT 'not_generated',
                    generated_view_name TEXT,
                    generated_form_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    superset_dataset_id BIGINT,
                    superset_url TEXT,
                    validation_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS report_template_fields (
                    id UUID PRIMARY KEY,
                    template_id UUID REFERENCES report_templates(id) ON DELETE CASCADE,
                    field_key TEXT,
                    field_name TEXT NOT NULL,
                    display_order BIGINT NOT NULL DEFAULT 0,
                    required BOOLEAN NOT NULL DEFAULT true,
                    intentionally_unmapped BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(template_id, field_name)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS report_field_matches (
                    id UUID PRIMARY KEY,
                    template_id UUID REFERENCES report_templates(id) ON DELETE CASCADE,
                    field_key TEXT,
                    field_name TEXT NOT NULL,
                    source_view TEXT,
                    source_column TEXT,
                    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                    match_reason TEXT,
                    match_type TEXT,
                    field_status TEXT NOT NULL DEFAULT 'not_matched',
                    sensitive_warning TEXT,
                    candidates_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    manual_override BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(template_id, field_name)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS report_generations (
                    id UUID PRIMARY KEY,
                    template_id UUID REFERENCES report_templates(id) ON DELETE CASCADE,
                    generated_view_name TEXT NOT NULL,
                    superset_dataset_id BIGINT,
                    superset_url TEXT,
                    status TEXT NOT NULL DEFAULT 'generated',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS report_generation_runs (
                    id UUID PRIMARY KEY,
                    template_id UUID REFERENCES report_templates(id) ON DELETE CASCADE,
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    duration_ms BIGINT,
                    validation_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    generated_view_name TEXT,
                    superset_dataset_id BIGINT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS report_template_audit (
                    id UUID PRIMARY KEY,
                    template_id UUID REFERENCES report_templates(id) ON DELETE SET NULL,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute("ALTER TABLE report_template_fields ADD COLUMN IF NOT EXISTS field_key TEXT")
            cursor.execute("ALTER TABLE report_template_fields ADD COLUMN IF NOT EXISTS intentionally_unmapped BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE report_field_matches ADD COLUMN IF NOT EXISTS field_key TEXT")
            cursor.execute("ALTER TABLE report_templates ADD COLUMN IF NOT EXISTS generated_form_json JSONB NOT NULL DEFAULT '{}'::jsonb")
            cursor.execute("ALTER TABLE report_generation_runs ADD COLUMN IF NOT EXISTS progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE report_generation_runs ADD COLUMN IF NOT EXISTS progress_step TEXT")
            cursor.execute("ALTER TABLE report_generation_runs ADD COLUMN IF NOT EXISTS current_action TEXT")
            cursor.execute("ALTER TABLE report_generation_runs ADD COLUMN IF NOT EXISTS timings_json JSONB NOT NULL DEFAULT '{}'::jsonb")
            cursor.execute("ALTER TABLE report_generation_runs ADD COLUMN IF NOT EXISTS superset_status TEXT")
            cursor.execute("ALTER TABLE report_generation_runs ADD COLUMN IF NOT EXISTS superset_error_message TEXT")
            cursor.execute("ALTER TABLE report_generation_runs ADD COLUMN IF NOT EXISTS progress_updated_at TIMESTAMPTZ")
            cursor.execute("UPDATE report_template_fields SET field_key = field_name WHERE field_key IS NULL")
            cursor.execute("UPDATE report_field_matches SET field_key = field_name WHERE field_key IS NULL")
            cursor.execute("ALTER TABLE report_template_fields DROP CONSTRAINT IF EXISTS report_template_fields_template_id_field_name_key")
            cursor.execute("ALTER TABLE report_field_matches DROP CONSTRAINT IF EXISTS report_field_matches_template_id_field_name_key")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_templates_status ON report_templates(status)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_report_template_fields_key ON report_template_fields(template_id, field_key)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_report_field_matches_key ON report_field_matches(template_id, field_key)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_matches_template ON report_field_matches(template_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_generation_runs_template ON report_generation_runs(template_id, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_generation_runs_status ON report_generation_runs(status, progress_updated_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_template_audit_template ON report_template_audit(template_id, created_at DESC)")


def audit_event(template_id: str | None, event_type: str, message: str, details: dict[str, Any] | None = None) -> None:
    try:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO report_template_audit (id, template_id, event_type, message, details_json)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (new_id(), template_id, event_type, message, Json(json_safe(details or {}))),
                )
    except Exception:
        pass


def timing_ms(started_at: float) -> int:
    return int((time.monotonic() - started_at) * 1000)


def log_generation_timing(run_id: str, template_id: str, timings: dict[str, int]) -> None:
    print(
        safe_json_dumps(
            {
                "event": "report_generation_timing",
                "run_id": run_id,
                "template_id": template_id,
                "timings_ms": timings,
            },
            sort_keys=True,
        )
    )


def update_generation_run_progress(
    run_id: str,
    *,
    step: str,
    progress_percent: float | None = None,
    current_action: str | None = None,
    timings: dict[str, int] | None = None,
    status: str | None = None,
    superset_status: str | None = None,
    superset_error_message: str | None = None,
    superset_dataset_id: int | None = None,
    error_message: str | None = None,
) -> None:
    try:
        fields = ["progress_step = %s", "progress_updated_at = now()"]
        values: list[Any] = [step]
        if progress_percent is not None:
            fields.append("progress_percent = %s")
            values.append(float(progress_percent))
        if current_action is not None:
            fields.append("current_action = %s")
            values.append(current_action)
        if timings is not None:
            fields.append("timings_json = %s")
            values.append(Json(json_safe(timings)))
        if status is not None:
            fields.append("status = %s")
            values.append(status)
        if superset_status is not None:
            fields.append("superset_status = %s")
            values.append(superset_status)
        if superset_error_message is not None:
            fields.append("superset_error_message = %s")
            values.append(superset_error_message)
        if superset_dataset_id is not None:
            fields.append("superset_dataset_id = %s")
            values.append(superset_dataset_id)
        if error_message is not None:
            fields.append("error_message = %s")
            values.append(error_message)
        values.append(run_id)
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"UPDATE report_generation_runs SET {', '.join(fields)} WHERE id = %s", values)
    except Exception:
        pass


def get_generation_run(run_id: str) -> dict[str, Any]:
    init_report_template_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM report_generation_runs WHERE id = %s", (run_id,))
            row = as_dict(cursor.fetchone())
    if not row:
        raise ValueError("Report generation run not found")
    started_at = row.get("started_at")
    finished_at = row.get("finished_at")
    if started_at:
        end = finished_at or utc_now()
        try:
            row["elapsed_time_ms"] = int((end - started_at).total_seconds() * 1000)
        except Exception:
            row["elapsed_time_ms"] = row.get("duration_ms") or 0
    return row


def source_view_row_count_from_cache(source_view: str) -> int:
    try:
        metadata = source_metadata()
        for view in metadata.get("views") or []:
            if view.get("view_name") == source_view:
                return int(view.get("row_count") or 0)
    except Exception:
        pass
    return 0


def field_key_for(field_name: str, index: int) -> str:
    return safe_identifier(f"field_{index + 1}", field_name, max_length=90)


def normalize_requested_fields(fields: list[Any] | None, matches: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if fields:
        normalized = []
        for index, item in enumerate(fields):
            if isinstance(item, dict):
                label = str(item.get("field_name") or item.get("label") or "").strip()
                key = str(item.get("field_key") or field_key_for(label, index)).strip()
                intentionally_unmapped = bool(item.get("intentionally_unmapped"))
                required = bool(item.get("required", True))
            else:
                label = str(item or "").strip()
                key = field_key_for(label, index)
                intentionally_unmapped = False
                required = True
            if label:
                normalized.append({"field_key": key, "field_name": label, "display_order": index, "required": required, "intentionally_unmapped": intentionally_unmapped})
        return normalized
    normalized = []
    for index, match in enumerate(matches or []):
        label = str(match.get("field_name") or "").strip()
        if label:
            normalized.append(
                {
                    "field_key": str(match.get("field_key") or field_key_for(label, index)),
                    "field_name": label,
                    "display_order": index,
                    "required": bool(match.get("required", True)),
                    "intentionally_unmapped": bool(match.get("intentionally_unmapped")),
                }
            )
    return normalized


def split_name_tokens(value: str) -> list[str]:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(value or ""))
    spaced = re.sub(r"[^A-Za-z0-9]+", " ", spaced).lower()
    tokens = [token for token in spaced.split() if token and token not in STOP_WORDS]
    normalized = []
    for token in tokens:
        token = ABBREVIATIONS.get(token, token)
        if token in {"status", "business"}:
            pass
        elif len(token) > 4 and token.endswith("ies"):
            token = f"{token[:-3]}y"
        elif len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
            token = token[:-1]
        normalized.append(token)
    return normalized


def core_name_tokens(value: str) -> list[str]:
    tokens = split_name_tokens(value)
    if len(tokens) <= 1:
        return tokens
    core = [token for token in tokens if token not in NOISE_WORDS]
    if len(core) > 1:
        without_suffix = [token for token in core if token not in COMMON_SUFFIX_TOKENS]
        if without_suffix:
            core = without_suffix
    return core or tokens


def expanded_tokens(value: str) -> set[str]:
    tokens = set(split_name_tokens(value)) | set(core_name_tokens(value))
    expanded = set(tokens)
    for token in tokens:
        expanded.update(TOKEN_SYNONYMS.get(token, ()))
    return expanded


def normalize_field(value: str) -> str:
    return "".join(split_name_tokens(value))


def normalize_core_field(value: str) -> str:
    return "".join(core_name_tokens(value))


def normalize_compact(value: str) -> str:
    """Case-insensitive column identity: remove spaces, separators, and punctuation."""
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def semantic_key(value: str) -> str:
    return " ".join(split_name_tokens(value))


def alias_norms_for(field_name: str) -> set[str]:
    semantic = semantic_key(field_name)
    aliases = set(SEMANTIC_ALIASES.get(semantic, ()))
    aliases.update(SEMANTIC_ALIASES.get(normalize_field(field_name), ()))
    aliases.update(SEMANTIC_ALIASES.get(normalize_core_field(field_name), ()))
    aliases.add(field_name)
    aliases.update("_".join(tokens) for tokens in [split_name_tokens(field_name)] if tokens)
    aliases.update("_".join(tokens) for tokens in [core_name_tokens(field_name)] if tokens)
    return {normalize_field(alias) for alias in aliases if alias}


def historical_alias_norms_for(field_name: str) -> set[str]:
    cache_key = normalize_field(field_name)
    if cache_key in _HISTORICAL_ALIAS_CACHE:
        return _HISTORICAL_ALIAS_CACHE[cache_key]
    aliases: set[str] = set()
    if not cache_key:
        _HISTORICAL_ALIAS_CACHE[cache_key] = aliases
        return aliases
    try:
        with dashboard_connection() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT source_column
                    FROM report_field_matches
                    WHERE source_column IS NOT NULL
                      AND lower(regexp_replace(field_name, '[^a-zA-Z0-9]+', '', 'g')) = %s
                    GROUP BY source_column
                    ORDER BY max(confidence) DESC, count(*) DESC
                    LIMIT 12
                    """,
                    (cache_key,),
                )
                aliases = {normalize_field(str(row["source_column"])) for row in cursor.fetchall() if row.get("source_column")}
    except Exception:
        aliases = set()
    _HISTORICAL_ALIAS_CACHE[cache_key] = aliases
    return aliases


def token_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    intersection = left & right
    overlap = len(intersection) / max(len(left), 1)
    dice = (2 * len(intersection)) / max(len(left) + len(right), 1)
    return max(overlap, dice)


def acronym_for(tokens: list[str] | set[str]) -> str:
    ordered = list(tokens)
    return "".join(token[:1] for token in ordered if token)


def partial_word_similarity(left_tokens: set[str], right_tokens: set[str]) -> float:
    if not left_tokens or not right_tokens:
        return 0.0
    hits = 0
    for left in left_tokens:
        if any(
            len(left) >= 4
            and len(right) >= 4
            and (left.startswith(right) or right.startswith(left) or left in right or right in left)
            for right in right_tokens
        ):
            hits += 1
    return hits / max(len(left_tokens), 1)


def score_category(confidence: float, *, selected: bool = False, missing: bool = False) -> str:
    percent = round(float(confidence or 0) * 100)
    if missing:
        return "Missing"
    if percent >= 95:
        return "Excellent Match"
    if percent >= 80:
        return "Good Match"
    if percent >= 60:
        return "Possible Match"
    return "Weak Match"


def match_priority(match_type: str | None) -> int:
    order = {
        "exact": 6,
        "normalized": 5,
        "business_alias": 4,
        "historical": 4,
        "acronym": 3,
        "partial_word": 2,
        "token": 1,
    }
    return order.get(str(match_type or ""), 0)


def clean_column_priority(column_name: str) -> tuple[int, int]:
    value = str(column_name or "")
    punctuation = len(re.findall(r"[^A-Za-z0-9]", value))
    return (-len(value), -punctuation)


def strip_hash_suffix(value: str) -> str:
    return re.sub(r"(_hash|hash)$", "", str(value or ""), flags=re.IGNORECASE)


def is_platform_metadata_column(column_name: str) -> bool:
    value = str(column_name or "").lower()
    if value in {"source_id", "source_name", "source_database", "source_collection", "raw_run_id", "raw_file_id", "raw_object_key"}:
        return True
    return value.startswith(("bronze_", "silver_"))


def is_child_view(view_name: str, source_table: str | None = None) -> bool:
    probe = f"{view_name} {source_table or ''}".lower()
    return any(marker in probe for marker in CHILD_TABLE_MARKERS)


def sensitive_warning_for(field_name: str, column_name: str | None) -> str | None:
    probe = f"{field_name} {column_name or ''}".lower()
    if column_name and column_name.lower() in {"source_name", "source_type", "source_system", "database_name", "collection_name"}:
        return None
    if column_name and column_name.lower().endswith("_hash"):
        return "This field maps to a protected hashed value in a Silver safe view."
    if column_name and is_pii_column(column_name):
        return "This field may contain sensitive data. Confirm it is allowed in this report."
    if any(marker in probe for marker in SENSITIVE_MARKERS):
        return "This field may contain sensitive data. Confirm it is allowed in this report."
    return None


def is_exact_sensitive_request(field_name: str, column_name: str) -> bool:
    field_norm = normalize_field(field_name)
    column_norm = normalize_field(column_name)
    column_base_norm = normalize_field(strip_hash_suffix(column_name))
    if field_norm in {column_norm, column_base_norm}:
        return True
    alias_norms = alias_norms_for(field_name)
    if column_norm in alias_norms or column_base_norm in alias_norms:
        return True
    field_tokens = expanded_tokens(field_name)
    column_tokens = expanded_tokens(strip_hash_suffix(column_name))
    sensitive_tokens = {"email", "phone", "mobile", "passport", "emirates", "dob", "birth", "name"}
    return bool((field_tokens & column_tokens) & sensitive_tokens)


def columns_from_schema_snapshot(source_table: str | None) -> list[str]:
    if not source_table:
        return []
    try:
        with dashboard_connection() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT fields_json
                    FROM silver_schema_snapshots
                    WHERE silver_table_name = %s
                    ORDER BY detected_at DESC
                    LIMIT 1
                    """,
                    (source_table,),
                )
                row = as_dict(cursor.fetchone())
                if not row:
                    return []
                fields_json = row.get("fields_json") or {}
                if isinstance(fields_json, str):
                    fields_json = json.loads(fields_json)
                fields = fields_json.get("fields") if isinstance(fields_json, dict) else fields_json
                columns = []
                for item in fields or []:
                    if isinstance(item, dict):
                        name = item.get("name") or item.get("field") or item.get("field_path")
                    else:
                        name = str(item).split(":", 1)[0]
                    if name:
                        columns.append(str(name))
                return columns
    except Exception:
        return []


def columns_from_field_profiles(source_table: str | None) -> list[str]:
    if not source_table:
        return []
    try:
        with dashboard_connection() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT field_path
                    FROM silver_field_profiles
                    WHERE table_name = %s
                      AND (extracted_as_column = true OR raw_json_fallback = false)
                    ORDER BY occurrence_percent DESC, field_path
                    LIMIT 250
                    """,
                    (source_table,),
                )
                return [str(row["field_path"]).replace(".", "_").replace("[", "_").replace("]", "") for row in cursor.fetchall() if row.get("field_path")]
    except Exception:
        return []


def field_profiles_for_table(source_table: str | None) -> dict[str, dict[str, Any]]:
    if not source_table:
        return {}
    try:
        with dashboard_connection() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT field_path, detected_type, occurrence_percent, occurrence_count, pii_detected
                    FROM silver_field_profiles
                    WHERE table_name = %s
                    """,
                    (source_table,),
                )
                profiles = {}
                for row in cursor.fetchall():
                    item = as_dict(row)
                    field_path = str(item.get("field_path") or "")
                    if not field_path:
                        continue
                    aliases = {
                        normalize_field(field_path),
                        normalize_field(field_path.replace(".", "_").replace("[", "_").replace("]", "")),
                    }
                    for alias in aliases:
                        if alias:
                            profiles[alias] = item
                return profiles
    except Exception:
        return {}


def candidate_columns_for_view(view: dict[str, Any]) -> tuple[list[str], str]:
    columns = [str(column) for column in view.get("columns") or [] if column]
    if columns:
        return columns, "safe_view_columns"
    source_table = view.get("source_table")
    snapshot_columns = columns_from_schema_snapshot(source_table)
    if snapshot_columns:
        return snapshot_columns, "silver_schema_snapshot"
    profile_columns = columns_from_field_profiles(source_table)
    if profile_columns:
        return profile_columns, "silver_field_profiles"
    return [], "none"


def safe_view_column_details(view_name: str, fallback_columns: list[str]) -> list[dict[str, Any]]:
    try:
        _, rows, _ = trino_query(f"SHOW COLUMNS FROM delta.silver.{quote_identifier(view_name)}")
        details = []
        for row in rows:
            name = str(row[0]) if row else ""
            if not name:
                continue
            details.append({"name": name, "data_type": str(row[1]) if len(row) > 1 and row[1] else "unknown"})
        if details:
            return details
    except Exception:
        pass
    return [{"name": str(column), "data_type": "unknown"} for column in fallback_columns if column]


def view_index_by_name(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(view.get("view_name")): view for view in metadata.get("views") or [] if view.get("view_name")}


def lightweight_metadata_for_template_sources(template: dict[str, Any]) -> dict[str, Any]:
    views = []
    for source_view in sorted({match["source_view"] for match in selected_matches(template) if match.get("source_view")}):
        views.append(
            {
                "view_name": source_view,
                "source_table": source_view,
                "columns": [],
                "column_index": [],
                "tokens": sorted(expanded_tokens(source_view)),
                "row_count": 0,
                "column_metadata_source": "live_trino",
            }
        )
    return {
        "status": "ok" if views else "missing_source_dataset",
        "message": "Template source views loaded for live validation.",
        "source": "template_selected_views",
        "views": views,
        "missing_safe_views": [],
        "skipped_safe_views": [],
    }


def view_column_names(view: dict[str, Any]) -> list[str]:
    names = []
    for column in view.get("column_index") or view.get("columns") or []:
        if isinstance(column, dict):
            name = str(column.get("source_column") or column.get("column_name") or column.get("name") or "")
        else:
            name = str(column or "")
        if name:
            names.append(name)
    return names


def columns_for_validation(source_view: str, metadata: dict[str, Any], *, live_check: bool = True) -> tuple[list[dict[str, Any]], str]:
    view = view_index_by_name(metadata).get(source_view) or {}
    fallback_columns = view_column_names(view)
    if live_check:
        details = safe_view_column_details(source_view, fallback_columns)
        return details, "trino_show_columns" if details else "trino_show_columns_empty"
    return [{"name": column, "data_type": "unknown"} for column in fallback_columns], str(view.get("column_metadata_source") or "metadata_cache")


def sql_fragment_for_mapping(match: dict[str, Any]) -> str:
    if match.get("source_column"):
        return f"{quote_identifier(match['source_column'])} AS {quote_identifier(match['field_name'])}"
    return f"CAST(NULL AS varchar) AS {quote_identifier(match.get('field_name') or 'unmapped')}"


def same_table_column_alternatives(
    field_name: str,
    source_view: str,
    missing_column: str,
    metadata: dict[str, Any],
    *,
    used_pairs: set[tuple[str, str]] | None = None,
    fallback_details: list[dict[str, Any]] | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    used_pairs = used_pairs or set()
    view = view_index_by_name(metadata).get(source_view)
    if fallback_details and (not view or not view_column_names(view)):
        view = {
            **(view or {}),
            "view_name": source_view,
            "source_table": (view or {}).get("source_table") or source_view,
            "columns": [str(item.get("name") or "") for item in fallback_details if item.get("name")],
            "column_index": [
                {
                    "source_column": str(item.get("name") or ""),
                    "data_type": str(item.get("data_type") or "unknown"),
                    "tokens": expanded_tokens(str(item.get("name") or "")),
                }
                for item in fallback_details
                if item.get("name")
            ],
            "tokens": sorted(expanded_tokens(source_view)),
        }
    if not view:
        return []
    scored_by_column = {
        str(candidate.get("source_column")): candidate
        for candidate in field_candidates(field_name, [view], preferred_view=source_view, used_pairs=used_pairs)
        if candidate.get("source_column") and str(candidate.get("source_column")) != missing_column
    }
    missing_norm = normalize_field(missing_column)
    for column in view.get("column_index") or view.get("columns") or []:
        column_name = str(column.get("source_column") if isinstance(column, dict) else column)
        if not column_name or column_name == missing_column or (source_view, column_name) in used_pairs:
            continue
        similarity = difflib.SequenceMatcher(None, missing_norm, normalize_field(column_name)).ratio()
        existing = scored_by_column.get(column_name)
        if existing:
            existing["confidence"] = round(max(float(existing.get("confidence") or 0), min(0.92, similarity)), 3)
            existing["match_reason"] = f"{existing.get('match_reason')}; same-table repair candidate"
        elif similarity >= 0.45:
            scored_by_column[column_name] = score_candidate(field_name, view, column, preferred_view=source_view)
            scored_by_column[column_name]["confidence"] = round(min(0.82, similarity), 3)
            scored_by_column[column_name]["match_reason"] = f"Similar to missing column {missing_column}; same-table repair candidate"
            scored_by_column[column_name]["match_type"] = "same_table_repair"
    return sorted(
        scored_by_column.values(),
        key=lambda item: (
            float(item.get("confidence") or 0),
            match_priority(item.get("match_type")),
            *clean_column_priority(str(item.get("source_column") or "")),
        ),
        reverse=True,
    )[:limit]


def validate_mapped_columns_against_schema(
    template: dict[str, Any],
    metadata: dict[str, Any],
    *,
    live_check: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    checked_views: dict[str, dict[str, Any]] = {}
    view_index = view_index_by_name(metadata)
    for source_view in sorted({match["source_view"] for match in selected_matches(template) if match.get("source_view")}):
        details, source = columns_for_validation(source_view, metadata, live_check=live_check)
        names = [str(item.get("name") or "") for item in details if item.get("name")]
        checked_views[source_view] = {
            "columns": names,
            "details": details,
            "source": source,
            "metadata_known": source_view in view_index,
        }
    used_pairs = {(match["source_view"], match["source_column"]) for match in selected_matches(template)}
    for match in selected_matches(template):
        source_view = str(match.get("source_view") or "")
        source_column = str(match.get("source_column") or "")
        checked = checked_views.get(source_view) or {}
        columns = checked.get("columns") or []
        if not checked.get("metadata_known") and not columns:
            errors.append(
                {
                    "field": match.get("field_name"),
                    "field_key": match.get("field_key"),
                    "type": "missing_source_view",
                    "message": f"Selected table/view no longer exists: {source_view}.",
                    "source_view": source_view,
                    "source_column": source_column,
                    "requested_field": match.get("field_name"),
                    "sql_fragment": sql_fragment_for_mapping(match),
                }
            )
            continue
        if source_column not in columns:
            alternatives = same_table_column_alternatives(
                str(match.get("field_name") or ""),
                source_view,
                source_column,
                metadata,
                used_pairs=used_pairs - {(source_view, source_column)},
                fallback_details=checked.get("details") or [],
            )
            errors.append(
                {
                    "field": match.get("field_name"),
                    "field_key": match.get("field_key"),
                    "type": "missing_mapped_column",
                    "message": f"Column no longer exists: {source_view}.{source_column}.",
                    "source_view": source_view,
                    "source_column": source_column,
                    "requested_field": match.get("field_name"),
                    "available_columns": columns[:75],
                    "candidate_replacements": [
                        {
                            "source_view": item.get("source_view"),
                            "source_column": item.get("source_column"),
                            "confidence": item.get("confidence"),
                            "match_type": item.get("match_type"),
                            "match_reason": item.get("match_reason"),
                        }
                        for item in alternatives
                    ],
                    "sql_fragment": sql_fragment_for_mapping(match),
                }
            )
    return errors, checked_views


def auto_fix_invalid_mapped_columns(
    template_id: str,
    *,
    metadata: dict[str, Any] | None = None,
    time_budget_ms: int = 2500,
) -> dict[str, Any] | None:
    started = time.monotonic()
    metadata = metadata or source_metadata()
    template = get_template(template_id)
    schema_errors, _ = validate_mapped_columns_against_schema(template, metadata, live_check=True)
    invalid = [error for error in schema_errors if error.get("type") == "missing_mapped_column"]
    if not invalid:
        return None
    repaired = []
    unresolved = []
    used_pairs = {(match["source_view"], match["source_column"]) for match in selected_matches(template)}
    match_by_key = {str(match.get("field_key") or ""): match for match in template.get("matches") or []}
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for error in invalid:
                elapsed_ms = timing_ms(started)
                if elapsed_ms >= time_budget_ms:
                    unresolved.append({**error, "reason": f"Auto repair budget exhausted after {elapsed_ms} ms"})
                    continue
                match = match_by_key.get(str(error.get("field_key") or ""))
                if not match:
                    unresolved.append({**error, "reason": "Saved mapping row was not found"})
                    continue
                alternatives = same_table_column_alternatives(
                    str(match.get("field_name") or ""),
                    str(match.get("source_view") or ""),
                    str(match.get("source_column") or ""),
                    metadata,
                    used_pairs=used_pairs - {(match.get("source_view"), match.get("source_column"))},
                    limit=10,
                )
                replacement = next((item for item in alternatives if float(item.get("confidence") or 0) >= REPORT_AUTO_FIX_MIN_CONFIDENCE), None)
                if replacement:
                    used_pairs.discard((match.get("source_view"), match.get("source_column")))
                    used_pairs.add((replacement["source_view"], replacement["source_column"]))
                    _update_match_from_candidate(
                        cursor,
                        template_id,
                        str(match.get("field_key")),
                        replacement,
                        f"Auto-repaired stale mapping: {match.get('source_view')}.{match.get('source_column')} no longer exists; rematched to {replacement['source_view']}.{replacement['source_column']}.",
                    )
                    repaired.append(
                        {
                            "field_key": match.get("field_key"),
                            "field_name": match.get("field_name"),
                            "from_source_view": match.get("source_view"),
                            "from_source_column": match.get("source_column"),
                            "to_source_view": replacement.get("source_view"),
                            "to_source_column": replacement.get("source_column"),
                            "confidence": replacement.get("confidence"),
                        }
                    )
                else:
                    unresolved.append({**error, "reason": "No same-table candidate reached confidence threshold"})
            cursor.execute("UPDATE report_templates SET updated_at = now() WHERE id = %s", (template_id,))
    details = {
        "action": "auto_fix_invalid_mapped_columns",
        "fields_checked": len(invalid),
        "fields_repaired": repaired,
        "fields_unresolved": unresolved,
        "duration_ms": timing_ms(started),
    }
    audit_event(template_id, "invalid_mapping_auto_fix", "Invalid report mappings auto-repaired where possible", details)
    return details


def expected_data_type_group(field_name: str) -> str | None:
    tokens = expanded_tokens(field_name)
    normalized = normalize_field(field_name)
    if tokens & {"date", "timestamp", "created", "submitted", "assigned", "start", "end"} or normalized.endswith("at"):
        return "temporal"
    if tokens & {"time", "duration", "elapsed", "period", "sla", "waiting", "queue", "pending"}:
        return "duration"
    if tokens & {"number", "no", "num", "id", "code", "reference"}:
        return "identifier"
    if tokens & {"status", "state", "stage", "phase", "type", "category", "name", "company", "service", "team"}:
        return "text"
    return None


def actual_data_type_group(data_type: str | None) -> str | None:
    value = str(data_type or "").lower()
    if not value or value == "unknown":
        return None
    if any(marker in value for marker in ("timestamp", "date", "time")):
        return "temporal"
    if any(marker in value for marker in ("int", "decimal", "double", "real", "numeric", "bigint", "smallint", "float")):
        return "number"
    if any(marker in value for marker in ("char", "varchar", "string")):
        return "text"
    if "bool" in value:
        return "boolean"
    return "complex" if any(marker in value for marker in ("array", "map", "row", "json")) else None


def data_type_compatibility(expected: str | None, actual: str | None) -> tuple[float, str | None]:
    if not expected or not actual:
        return 0.0, None
    if expected == actual:
        return 0.04, "compatible type"
    if expected == "duration" and actual in {"number", "temporal", "text"}:
        return 0.03, "compatible duration type"
    if expected == "identifier" and actual in {"text", "number"}:
        return 0.03, "compatible identifier type"
    if expected == "temporal" and actual == "text":
        return 0.01, "possible temporal text"
    if expected == "text" and actual in {"text", "number"}:
        return 0.02, "compatible label type"
    return -0.08, "datatype may not match"


def index_column(view: dict[str, Any], column: dict[str, Any]) -> dict[str, Any]:
    column_name = str(column.get("name") or column.get("column_name") or "")
    base_name = strip_hash_suffix(column_name)
    aliases = alias_norms_for(column_name)
    aliases.add(normalize_core_field(column_name))
    sensitivity_flags = {
        "pii": bool(is_pii_column(column_name)),
        "hashed": column_name.lower().endswith("_hash"),
        "platform_metadata": is_platform_metadata_column(column_name),
    }
    return {
        "source_view": view.get("view_name"),
        "source_table": view.get("source_table"),
        "source_kind": view.get("source_kind"),
        "source_column": column_name,
        "column_name": column_name,
        "data_type": column.get("data_type") or "unknown",
        "normalized_name": normalize_field(column_name),
        "normalized_base_name": normalize_field(base_name),
        "core_normalized_name": normalize_core_field(base_name),
        "aliases": sorted(alias for alias in aliases if alias),
        "tokens": sorted(expanded_tokens(base_name)),
        "raw_tokens": sorted(split_name_tokens(base_name)),
        "sample_profile": column.get("sample_profile") or {},
        "sensitivity_flags": sensitivity_flags,
        "is_sensitive": any(sensitivity_flags.values()),
        "row_count": int(view.get("row_count") or 0),
        "database_name": view.get("database_name"),
        "collection_name": view.get("collection_name"),
        "source_context": {
            "database_name": view.get("database_name"),
            "collection_name": view.get("collection_name"),
            "source_table": view.get("source_table"),
            "source_kind": view.get("source_kind"),
        },
    }


def build_view_index(view: dict[str, Any], *, enrich_profiles: bool = True) -> dict[str, Any]:
    column_details = list(view.get("column_details") or [])
    if not column_details:
        column_details = safe_view_column_details(view["view_name"], view.get("columns") or [])
    if enrich_profiles:
        profile_by_column = field_profiles_for_table(view.get("source_table"))
        for column in column_details:
            profile = profile_by_column.get(normalize_field(str(column.get("name") or "")))
            if profile:
                column["sample_profile"] = profile
                if column.get("data_type") in {None, "", "unknown"} and profile.get("detected_type"):
                    column["data_type"] = profile.get("detected_type")
    column_index = [index_column(view, column) for column in column_details]
    view_tokens = expanded_tokens(
        f"{view.get('view_name')} {view.get('source_table')} {view.get('collection_name')} {view.get('database_name')}"
    )
    return {
        **view,
        "columns": [column["source_column"] for column in column_index],
        "column_details": column_details,
        "column_index": column_index,
        "normalized_view_name": normalize_field(str(view.get("view_name") or "")),
        "tokens": sorted(view_tokens),
        "row_count": int(view.get("row_count") or 0),
    }


def source_metadata() -> dict[str, Any]:
    now = time.time()
    cached = _SAFE_VIEW_METADATA_CACHE.get("metadata")
    if cached and now < float(_SAFE_VIEW_METADATA_CACHE.get("expires_at") or 0):
        metadata = copy.deepcopy(cached)
        metadata["cache_status"] = "hit"
        return metadata
    metadata = _source_metadata_uncached()
    _SAFE_VIEW_METADATA_CACHE["metadata"] = copy.deepcopy(metadata)
    _SAFE_VIEW_METADATA_CACHE["expires_at"] = now + SAFE_VIEW_METADATA_CACHE_SECONDS
    metadata = copy.deepcopy(metadata)
    metadata["cache_status"] = "miss"
    return metadata


def clear_source_metadata_cache() -> None:
    _SAFE_VIEW_METADATA_CACHE["metadata"] = None
    _SAFE_VIEW_METADATA_CACHE["expires_at"] = 0.0
    _METADATA_LAYER_CACHE["metadata"] = None
    _METADATA_LAYER_CACHE["expires_at"] = 0.0
    _METADATA_LAYER_CACHE["loaded_at"] = None


def metadata_json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def schema_fields_to_column_details(fields_json: Any) -> list[dict[str, Any]]:
    fields_json = metadata_json_value(fields_json, {})
    fields = fields_json.get("fields") if isinstance(fields_json, dict) else fields_json
    column_details: list[dict[str, Any]] = []
    for item in fields or []:
        if isinstance(item, dict):
            name = item.get("name") or item.get("field") or item.get("field_path")
            data_type = item.get("type") or item.get("data_type") or item.get("detected_type") or "unknown"
            business_label = item.get("business_label") or item.get("label") or item.get("display_name")
        else:
            parts = str(item).split(":", 1)
            name = parts[0]
            data_type = parts[1] if len(parts) > 1 else "unknown"
            business_label = None
        if name:
            column_details.append(
                {
                    "name": str(name),
                    "data_type": str(data_type or "unknown"),
                    "business_label": business_label,
                    "metadata_source": "silver_schema_snapshots",
                }
            )
    return column_details


def profiles_to_column_details(profiles_json: Any) -> list[dict[str, Any]]:
    profiles = metadata_json_value(profiles_json, [])
    column_details = []
    for profile in profiles or []:
        if not isinstance(profile, dict):
            continue
        field_path = str(profile.get("field_path") or "")
        if not field_path:
            continue
        column_details.append(
            {
                "name": field_path.replace(".", "_").replace("[", "_").replace("]", ""),
                "data_type": str(profile.get("detected_type") or "unknown"),
                "business_label": profile.get("business_label") or profile.get("original_field_path") or field_path,
                "sample_profile": profile,
                "metadata_source": "silver_field_profiles",
            }
        )
    return column_details


def dedupe_column_details(column_details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for column in column_details:
        name = str(column.get("name") or "")
        if not name:
            continue
        key = normalize_field(name)
        existing = by_name.get(key)
        if not existing:
            by_name[key] = column
            continue
        if existing.get("data_type") in {None, "", "unknown"} and column.get("data_type"):
            existing["data_type"] = column.get("data_type")
        if not existing.get("sample_profile") and column.get("sample_profile"):
            existing["sample_profile"] = column.get("sample_profile")
        if not existing.get("business_label") and column.get("business_label"):
            existing["business_label"] = column.get("business_label")
    return list(by_name.values())


def metadata_layer_profile_columns_for_tables(table_names: set[str]) -> dict[str, list[dict[str, Any]]]:
    names = sorted({str(name) for name in table_names if name})
    if not names:
        return {}
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT table_name,
                       field_path,
                       detected_type,
                       occurrence_count,
                       occurrence_percent,
                       pii_detected,
                       extracted_as_column,
                       raw_json_fallback
                FROM silver_field_profiles
                WHERE table_name = ANY(%s)
                  AND (extracted_as_column = true OR raw_json_fallback = false)
                ORDER BY table_name, occurrence_percent DESC, field_path
                """,
                (names,),
            )
            by_table: dict[str, list[dict[str, Any]]] = {}
            for row in cursor.fetchall():
                item = as_dict(row)
                by_table.setdefault(str(item.get("table_name") or ""), []).append(item)
            return {table: profiles_to_column_details(profiles) for table, profiles in by_table.items()}


def metadata_layer_source_metadata() -> dict[str, Any]:
    started = time.perf_counter()
    rows: list[dict[str, Any]] = []
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                WITH latest_schema AS (
                    SELECT DISTINCT ON (silver_table_name)
                           silver_table_name,
                           fields_json,
                           detected_at
                    FROM silver_schema_snapshots
                    ORDER BY silver_table_name, detected_at DESC
                )
                SELECT qsv.*,
                       scs.silver_table_name AS canonical_silver_table,
                       scs.source_database,
                       scs.source_collection,
                       scs.is_child_table,
                       scs.parent_silver_table_name,
                       scs.child_path,
                       scs.table_classification,
                       scs.bi_suitability,
                       scs.governance_status,
                       latest_schema.fields_json,
                       latest_schema.detected_at AS schema_detected_at
                FROM query_safe_views qsv
                LEFT JOIN silver_collection_states scs
                  ON qsv.source_table IN (scs.silver_table_name, scs.trino_table_name)
                LEFT JOIN latest_schema
                  ON latest_schema.silver_table_name = COALESCE(scs.silver_table_name, qsv.source_table)
                ORDER BY qsv.view_name
                """
            )
            rows = [as_dict(row) for row in cursor.fetchall()]

    views = []
    skipped_views = []
    incomplete_views = []
    metadata_rows_loaded = len(rows)
    fallback_tables = {
        str(row.get("canonical_silver_table") or row.get("source_table") or "")
        for row in rows
        if row.get("status") == "ok"
        and row.get("trino_visible")
        and row.get("pii_safe")
        and not schema_fields_to_column_details(row.get("fields_json"))
    }
    profile_columns_by_table = metadata_layer_profile_columns_for_tables(fallback_tables)
    metadata_rows_loaded += sum(len(columns) for columns in profile_columns_by_table.values())
    for row in rows:
        view_name = str(row.get("view_name") or "")
        if not view_name:
            continue
        source_table = row.get("source_table") or source_table_for_safe_view(view_name)
        status = row.get("status", "unknown")
        if status != "ok" or not row.get("trino_visible") or not row.get("pii_safe"):
            skipped_views.append(
                {
                    "view_name": view_name,
                    "status": status,
                    "trino_visible": row.get("trino_visible"),
                    "pii_safe": row.get("pii_safe"),
                    "error_message": row.get("error_message"),
                }
            )
            continue
        schema_columns = schema_fields_to_column_details(row.get("fields_json"))
        profile_columns = profile_columns_by_table.get(str(row.get("canonical_silver_table") or source_table), [])
        column_details = dedupe_column_details([*schema_columns, *profile_columns])
        if not column_details:
            incomplete_views.append({"view_name": view_name, "source_table": source_table, "reason": "missing_column_metadata"})
            continue
        child = bool(row.get("is_child_table")) or is_child_view(view_name, source_table)
        scope = source_scope_from_table(source_table)
        views.append(
            build_view_index(
                {
                    "view_name": view_name,
                    "source_table": source_table,
                    "silver_table": source_table,
                    "safe_analytics_view": view_name,
                    **scope,
                    "database_name": row.get("source_database") or scope.get("database_name"),
                    "collection_name": row.get("source_collection") or scope.get("collection_name"),
                    "row_count": int(row.get("row_count") or 0),
                    "column_count": int(row.get("column_count") or len(column_details)),
                    "source_column_count": int(row.get("source_column_count") or 0),
                    "columns": [column["name"] for column in column_details],
                    "column_details": column_details,
                    "pii_safe": bool(row.get("pii_safe")),
                    "trino_visible": bool(row.get("trino_visible")),
                    "hash_version": row.get("hash_version") or QUERY_PII_HASH_VERSION,
                    "blocked_columns": list(row.get("blocked_columns_json") or []),
                    "pii_protection_status": "protected",
                    "mapping_status": "generated",
                    "safe_view_status": "available",
                    "last_validated_at": row.get("last_validated_at"),
                    "status": status,
                    "error_message": row.get("error_message"),
                    "schema_detected_at": row.get("schema_detected_at"),
                    "profile_column_count": len(profile_columns),
                    "column_metadata_source": "metadata_layer",
                    "source_kind": "child_expanded" if child else "parent_safe_view",
                    "display_name": f"{view_name} (expanded child table)" if child else f"{view_name} (parent safe view)",
                    "row_multiplication_risk": bool(child),
                    "relationship_metadata": {
                        "parent_silver_table_name": row.get("parent_silver_table_name"),
                        "child_path": row.get("child_path"),
                        "table_classification": row.get("table_classification"),
                        "bi_suitability": row.get("bi_suitability"),
                        "governance_status": row.get("governance_status"),
                    },
                },
                enrich_profiles=False,
            )
        )

    missing = [
        {
            "view_name": item.get("view_name"),
            "source_table": item.get("source_table"),
            "status": item.get("status") or "missing_column_metadata",
            "error_message": item.get("error_message") or item.get("reason"),
            "recommended_fix": "Metadata Layer is incomplete. Please refresh metadata.",
        }
        for item in [*skipped_views, *incomplete_views]
    ]
    dependency_status = "ok" if views else "metadata_incomplete"
    message = (
        "Safe view metadata loaded from the Metadata Layer."
        if views
        else "Metadata Layer is incomplete. Please refresh metadata."
    )
    return {
        "status": dependency_status,
        "message": message,
        "source": "metadata_layer",
        "views": views,
        "all_safe_view_count": len(rows),
        "usable_safe_view_count": len(views),
        "skipped_safe_views": skipped_views,
        "incomplete_metadata_views": incomplete_views,
        "missing_safe_views": missing,
        "metadata_rows_loaded": metadata_rows_loaded,
        "metadata_load_time_ms": round((time.perf_counter() - started) * 1000),
        "cache_status": "metadata_layer",
        "example_fields": EXAMPLE_TEMPLATE_FIELDS,
    }


def cached_metadata_layer_source_metadata(*, require_cached: bool = False, metadata_override: dict[str, Any] | None = None) -> dict[str, Any]:
    if metadata_override and metadata_override.get("views"):
        metadata = copy.deepcopy(metadata_override)
        metadata.setdefault("status", "ok")
        metadata.setdefault("message", "Using frontend cached metadata.")
        metadata.setdefault("source", "frontend_memory_cache")
        metadata["cache_status"] = "frontend_memory_cache"
        return metadata

    now = time.time()
    cached = _METADATA_LAYER_CACHE.get("metadata")
    if cached and (require_cached or now < float(_METADATA_LAYER_CACHE.get("expires_at") or 0)):
        metadata = copy.deepcopy(cached)
        metadata["cache_status"] = "metadata_layer_cache" if now < float(_METADATA_LAYER_CACHE.get("expires_at") or 0) else "metadata_layer_cache_stale"
        metadata["last_refreshed_at"] = _METADATA_LAYER_CACHE.get("loaded_at")
        return metadata

    if require_cached:
        raise RuntimeError("Metadata cache is empty. Click Refresh Metadata.")

    metadata = metadata_layer_source_metadata()
    loaded_at = datetime.now(timezone.utc).isoformat()
    _METADATA_LAYER_CACHE["metadata"] = copy.deepcopy(metadata)
    _METADATA_LAYER_CACHE["expires_at"] = now + SAFE_VIEW_METADATA_CACHE_SECONDS
    _METADATA_LAYER_CACHE["loaded_at"] = loaded_at
    metadata = copy.deepcopy(metadata)
    metadata["cache_status"] = "fresh"
    metadata["last_refreshed_at"] = loaded_at
    return metadata


def metadata_layer_cache_summary() -> dict[str, Any]:
    cached = _METADATA_LAYER_CACHE.get("metadata")
    if not cached:
        return {
            "metadataLoaded": False,
            "safeViewsCount": 0,
            "columnsCount": 0,
            "rowCount": 0,
            "lastRefreshedAt": None,
            "source": "missing",
            "trinoCalls": 0,
        }
    views = cached.get("views") or []
    return {
        "metadataLoaded": True,
        "safeViewsCount": len(views),
        "columnsCount": sum(len(view.get("column_index") or view.get("columns") or []) for view in views),
        "rowCount": int(cached.get("metadata_rows_loaded") or len(views)),
        "lastRefreshedAt": _METADATA_LAYER_CACHE.get("loaded_at"),
        "source": "metadata_layer_cache",
        "trinoCalls": 0,
    }


def sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def safe_view_columns_bulk(view_names: list[str]) -> dict[str, list[dict[str, str]]]:
    names = sorted({str(name) for name in view_names if name})
    if not names:
        return {}
    try:
        in_list = ", ".join(sql_string_literal(name) for name in names)
        _, rows, _ = trino_query(
            f"""
            SELECT table_name, column_name, data_type
            FROM delta.information_schema.columns
            WHERE table_schema = 'silver'
              AND table_name IN ({in_list})
            ORDER BY table_name, ordinal_position
            """
        )
        columns_by_view: dict[str, list[dict[str, str]]] = {name: [] for name in names}
        for row in rows:
            if len(row) < 2:
                continue
            table_name = str(row[0])
            column_name = str(row[1])
            data_type = str(row[2]) if len(row) > 2 and row[2] else "unknown"
            if table_name and column_name:
                columns_by_view.setdefault(table_name, []).append({"name": column_name, "data_type": data_type})
        return columns_by_view
    except Exception:
        return {}


def report_safe_views_detail() -> list[dict[str, Any]]:
    metadata_rows = safe_views_metadata()
    view_names = [str(row.get("view_name") or "") for row in metadata_rows if row.get("view_name")]
    column_details_by_view = safe_view_columns_bulk(view_names)
    rows = []
    for row in metadata_rows:
        view_name = str(row.get("view_name") or "")
        if not view_name:
            continue
        source_table = row.get("source_table") or source_table_for_safe_view(view_name)
        column_details = column_details_by_view.get(view_name) or []
        columns = [detail["name"] for detail in column_details if detail.get("name")]
        scope = source_scope_from_table(source_table)
        pii_safe = bool(row.get("pii_safe"))
        status = row.get("status", "unknown")
        if status != "ok":
            pii_status = "missing_safe_view" if status in {"missing_source", "unknown"} else "needs_attention"
        else:
            safe, _ = pii_safe_columns(columns)
            pii_status = "protected" if safe and pii_safe else "exposed"
        rows.append(
            {
                "view_name": view_name,
                "source_table": source_table,
                "silver_table": source_table,
                "safe_analytics_view": view_name,
                **scope,
                "row_count": int(row.get("row_count") or 0),
                "column_count": int(row.get("column_count") or len(columns)),
                "source_column_count": int(row.get("source_column_count") or 0),
                "columns": columns,
                "column_details": column_details,
                "pii_safe": pii_safe,
                "trino_visible": bool(row.get("trino_visible")) if row.get("trino_visible") is not None else bool(columns),
                "hash_version": row.get("hash_version") or QUERY_PII_HASH_VERSION,
                "blocked_columns": list(row.get("blocked_columns_json") or []),
                "pii_protection_status": pii_status,
                "mapping_status": "generated" if status == "ok" else "missing" if status in {"missing_source", "unknown"} else "failed",
                "safe_view_status": "available" if status == "ok" else status,
                "last_validated_at": row.get("last_validated_at"),
                "status": status,
                "error_message": row.get("error_message"),
            }
        )
    return rows


def _source_metadata_uncached() -> dict[str, Any]:
    init_report_template_db()
    views = []
    raw_views = report_safe_views_detail()
    skipped_views = []
    for view in raw_views:
        if view.get("status") != "ok" or not view.get("trino_visible") or not view.get("pii_safe"):
            skipped_views.append(
                {
                    "view_name": view.get("view_name"),
                    "status": view.get("status"),
                    "trino_visible": view.get("trino_visible"),
                    "pii_safe": view.get("pii_safe"),
                    "error_message": view.get("error_message"),
                }
            )
            continue
        view_name = view["view_name"]
        source_table = view.get("source_table")
        child = is_child_view(view_name, source_table)
        columns, column_source = candidate_columns_for_view(view)
        views.append(
            build_view_index(
                {
                    **view,
                    "columns": columns,
                    "column_metadata_source": column_source,
                    "source_kind": "child_expanded" if child else "parent_safe_view",
                    "display_name": f"{view_name} (expanded child table)" if child else f"{view_name} (parent safe view)",
                    "row_multiplication_risk": bool(child),
                }
            )
        )
    missing = []
    try:
        missing = missing_safe_views()
    except Exception:
        missing = []
    dependency_status = "ok" if views else "missing_safe_views"
    message = (
        "Safe views are available for report matching."
        if views
        else "No safe views available. Rebuild Query Safe Views first."
    )
    return {
        "status": dependency_status,
        "message": message,
        "source": "trino_silver_safe_views",
        "views": views,
        "all_safe_view_count": len(raw_views),
        "usable_safe_view_count": len(views),
        "skipped_safe_views": skipped_views,
        "missing_safe_views": missing,
        "example_fields": EXAMPLE_TEMPLATE_FIELDS,
    }


def score_candidate(field_name: str, view: dict[str, Any], column: str | dict[str, Any], *, preferred_view: str | None = None, duplicate: bool = False) -> dict[str, Any]:
    if isinstance(column, dict):
        column_name = str(column.get("source_column") or column.get("column_name") or column.get("name") or "")
        column_norm = str(column.get("normalized_name") or normalize_field(column_name))
        column_base_norm = str(column.get("normalized_base_name") or normalize_field(strip_hash_suffix(column_name)))
        column_core_norm = str(column.get("core_normalized_name") or normalize_core_field(strip_hash_suffix(column_name)))
        column_tokens = set(column.get("tokens") or expanded_tokens(strip_hash_suffix(column_name)))
        column_raw_tokens = set(column.get("raw_tokens") or split_name_tokens(strip_hash_suffix(column_name)))
        data_type = str(column.get("data_type") or "unknown")
        sensitivity_flags = dict(column.get("sensitivity_flags") or {})
        sample_profile = dict(column.get("sample_profile") or {})
    else:
        column_name = str(column or "")
        column_norm = normalize_field(column_name)
        column_base_norm = normalize_field(strip_hash_suffix(column_name))
        column_core_norm = normalize_core_field(strip_hash_suffix(column_name))
        column_tokens = expanded_tokens(strip_hash_suffix(column_name))
        column_raw_tokens = set(split_name_tokens(strip_hash_suffix(column_name)))
        data_type = "unknown"
        sensitivity_flags = {}
        sample_profile = {}
    field_norm = normalize_field(field_name)
    field_core_norm = normalize_core_field(field_name)
    field_compact = normalize_compact(field_name)
    column_compact = normalize_compact(column_name)
    column_base_compact = normalize_compact(strip_hash_suffix(column_name))
    alias_norms = alias_norms_for(field_name)
    alias_compacts = {normalize_compact(alias) for alias in SEMANTIC_ALIASES.get(semantic_key(field_name), ())}
    historical_alias_norms = historical_alias_norms_for(field_name)
    field_tokens = expanded_tokens(field_name)
    field_raw_tokens = set(split_name_tokens(field_name))
    view_tokens = set(view.get("tokens") or expanded_tokens(f"{view.get('view_name')} {view.get('source_table')} {view.get('collection_name')} {view.get('database_name')}"))
    child = view.get("source_kind") == "child_expanded"
    reasons = []
    warnings = []
    original_field = str(field_name).strip()
    original_column = str(column_name).strip()

    # Scoring is intentionally tiered: exact and business-alias evidence wins first,
    # token/acronym evidence can be strong, and fuzzy edit distance is capped low so
    # random-but-similar column names do not outrank real report-template fields.
    if original_field.lower() in {original_column.lower(), strip_hash_suffix(column_name).strip().lower()}:
        confidence = 1.0
        match_type = "exact"
        reasons.append(f"Exact column name match: {original_field} -> {original_column}")
    elif field_compact in {column_compact, column_base_compact}:
        confidence = 0.97
        match_type = "normalized"
        reasons.append("Exact normalized column match")
    elif field_norm in {column_norm, column_base_norm} or field_core_norm and field_core_norm == column_core_norm:
        confidence = 0.90
        match_type = "normalized"
        reasons.append("Normalized abbreviation match")
    elif (
        column_norm in alias_norms
        or column_base_norm in alias_norms
        or column_core_norm in alias_norms
        or column_compact in alias_compacts
        or column_base_compact in alias_compacts
    ):
        confidence = 0.94
        match_type = "business_alias"
        reasons.append(f"Matched by business alias: {original_field} -> {original_column}")
    elif column_norm in historical_alias_norms or column_base_norm in historical_alias_norms:
        confidence = 0.92
        match_type = "historical"
        reasons.append(f"Matched by historical mapping: {original_field} -> {original_column}")
    else:
        token_score = token_similarity(field_tokens, column_tokens)
        raw_token_score = token_similarity(field_raw_tokens, column_raw_tokens)
        partial_score = partial_word_similarity(field_raw_tokens, column_raw_tokens)
        field_acronym = acronym_for(split_name_tokens(field_name))
        column_acronym = acronym_for(split_name_tokens(strip_hash_suffix(column_name)))
        acronym_match = bool(field_acronym and len(field_acronym) > 1 and field_acronym in {column_acronym, column_compact, column_base_compact})
        fuzzy_score = max(
            difflib.SequenceMatcher(None, field_norm, column_norm).ratio(),
            difflib.SequenceMatcher(None, field_norm, column_base_norm).ratio(),
            difflib.SequenceMatcher(None, field_core_norm, column_core_norm).ratio() if field_core_norm and column_core_norm else 0,
        )
        if acronym_match:
            confidence = 0.88
            match_type = "acronym"
            reasons.append(f"Acronym match: {original_field} -> {original_column}")
        elif raw_token_score >= 0.85 or token_score >= 0.82:
            confidence = min(0.89, 0.72 + (max(raw_token_score, token_score) * 0.19))
            match_type = "token"
            reasons.append("Strong token match")
        elif partial_score >= 0.67:
            confidence = min(0.84, 0.64 + (partial_score * 0.22))
            match_type = "partial_word"
            reasons.append("Partial word match")
        elif token_score >= 0.56:
            confidence = min(0.79, 0.53 + (token_score * 0.30))
            match_type = "token"
            reasons.append("Good token similarity")
        else:
            confidence = min(0.62, fuzzy_score)
            match_type = "fuzzy" if confidence >= CANDIDATE_MIN_CONFIDENCE else "weak"
            reasons.append("Weak fuzzy similarity" if confidence >= CANDIDATE_MIN_CONFIDENCE else "Low confidence fallback")

    type_delta, type_reason = data_type_compatibility(expected_data_type_group(field_name), actual_data_type_group(data_type))
    if type_reason:
        confidence = max(0.0, min(1.0, confidence + type_delta))
        reasons.append(type_reason)
    if sample_profile:
        profile_type = actual_data_type_group(str(sample_profile.get("detected_type") or data_type))
        expected_type = expected_data_type_group(field_name)
        occurrence = float(sample_profile.get("occurrence_percent") or 0)
        if expected_type and profile_type == expected_type:
            confidence = min(1.0, confidence + 0.025)
            reasons.append("sample/profile data type matches")
        elif expected_type and profile_type and profile_type != expected_type:
            confidence = max(0.0, confidence - 0.05)
            warnings.append("sample/profile data type may not match")
        if occurrence >= 0.8:
            confidence = min(1.0, confidence + 0.01)
            reasons.append("sample/profile data is consistently populated")
    view_relevance = token_similarity(field_tokens, view_tokens)
    if view_relevance >= 0.28:
        confidence = min(1.0, confidence + min(0.05, view_relevance * 0.08))
        reasons.append("high table relevance")
    if preferred_view and view.get("view_name") == preferred_view:
        if match_type in STRONG_MATCH_TYPES:
            confidence = min(1.0, confidence + 0.025)
        reasons.append("same safe view")
    elif preferred_view:
        confidence = max(0.0, confidence - 0.03)

    if match_type == "fuzzy":
        confidence = min(confidence, 0.69)
    elif match_type == "weak":
        confidence = min(confidence, 0.49)
    elif match_type == "partial_word":
        confidence = min(confidence, 0.84)
    elif match_type == "token" and not (field_raw_tokens & column_raw_tokens):
        confidence = min(confidence, 0.78)

    if child and any(token in field_norm for token in ("stakeholder", "visa", "license", "activit")):
        confidence = min(1.0, confidence + 0.04)
        reasons.append("child table context is relevant")
    if "status" in field_tokens and not (column_tokens & {"status", "state", "stage", "phase"}):
        confidence = min(confidence, 0.62)
        warnings.append("status token is missing from candidate column")
    if "company" in field_tokens and not (column_tokens & {"company", "business", "organization", "organisation", "account", "customer"}):
        confidence = min(confidence, 0.62)
        warnings.append("company token is missing from candidate column")
    if "service" in field_tokens and not (column_tokens & {"service", "process", "workflow", "request"}):
        confidence = min(confidence, 0.66)
        warnings.append("service token is missing from candidate column")
    if (sensitivity_flags.get("platform_metadata") or is_platform_metadata_column(column_name)) and not (field_tokens & {"source", "raw", "bronze", "silver"}):
        confidence = min(confidence, 0.55)
        warnings.append("platform metadata column is not a business-field match")
    if (sensitivity_flags.get("hashed") or column_name.lower().endswith("_hash")) and not field_name.lower().endswith("hash"):
        confidence = min(confidence, 0.91)
        warnings.append("protected hashed safe-view column")
    sensitive_warning = sensitive_warning_for(field_name, column_name)
    if sensitive_warning and not is_exact_sensitive_request(field_name, column_name):
        confidence = min(confidence, 0.82)
        warnings.append("sensitive field")
    if duplicate:
        confidence = max(0.0, confidence - 0.12)
        warnings.append("duplicate candidate")
    if match_type in {"business_alias", "historical"}:
        confidence = min(confidence, 0.94)

    return {
        "source_view": view["view_name"],
        "source_table": view.get("source_table"),
        "source_kind": view.get("source_kind"),
        "source_column": column_name,
        "data_type": data_type,
        "confidence": round(float(confidence), 3),
        "match_type": match_type,
        "match_category": score_category(confidence),
        "match_reason": "; ".join(dict.fromkeys([*reasons, *warnings])) or "Candidate scored by Smart Match",
        "match_reasons": list(dict.fromkeys(reasons)),
        "warnings": list(dict.fromkeys(warnings)),
        "column_metadata_source": view.get("column_metadata_source"),
        "sensitive_warning": sensitive_warning,
        "row_multiplication_risk": bool(view.get("row_multiplication_risk")),
        "row_count": view.get("row_count"),
        "database_name": view.get("database_name"),
        "collection_name": view.get("collection_name"),
        "same_safe_view": bool(preferred_view and view.get("view_name") == preferred_view),
    }


def table_candidate_scores(field_name: str, views: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = []
    field_norm = normalize_field(field_name)
    field_tokens = expanded_tokens(field_name)
    for view in views:
        column_scores: list[dict[str, Any]] = []
        for column in view.get("column_index") or view.get("columns") or []:
            scored = score_candidate(field_name, view, column)
            column_scores.append(scored)
        view_norm = normalize_field(f"{view.get('view_name')} {view.get('source_table')} {view.get('collection_name')} {view.get('database_name')}")
        view_tokens = set(view.get("tokens") or expanded_tokens(f"{view.get('view_name')} {view.get('source_table')} {view.get('collection_name')} {view.get('database_name')}"))
        domain_score = difflib.SequenceMatcher(None, field_norm, view_norm).ratio()
        domain_score = max(domain_score, token_similarity(field_tokens, view_tokens))
        best_columns = sorted(column_scores, key=lambda item: item["confidence"], reverse=True)[:3]
        best_column_score = best_columns[0]["confidence"] if best_columns else 0
        score = max(best_column_score, min(0.78, domain_score))
        if score >= 0.35:
            candidates.append(
                {
                    "source_view": view["view_name"],
                    "source_table": view.get("source_table"),
                    "source_kind": view.get("source_kind"),
                    "display_name": view.get("display_name"),
                    "confidence": round(score, 3),
                    "match_category": score_category(score),
                    "match_reason": "Same business domain" if domain_score >= best_column_score else "Likely table from similar fields",
                    "best_columns": best_columns,
                    "row_multiplication_risk": bool(view.get("row_multiplication_risk")),
                }
            )
    return sorted(candidates, key=lambda item: (item["confidence"], item["source_view"]), reverse=True)[:5]


def table_candidates_from_field_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_view: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        view_name = candidate.get("source_view")
        if not view_name:
            continue
        existing = by_view.get(view_name)
        if existing:
            existing["best_columns"].append(candidate)
            existing["best_columns"] = sorted(existing["best_columns"], key=lambda item: float(item.get("confidence") or 0), reverse=True)[:3]
            existing["confidence"] = max(float(existing.get("confidence") or 0), float(candidate.get("confidence") or 0))
            continue
        by_view[str(view_name)] = {
            "source_view": view_name,
            "source_table": candidate.get("source_table"),
            "source_kind": candidate.get("source_kind"),
            "display_name": candidate.get("display_name") or view_name,
            "confidence": float(candidate.get("confidence") or 0),
            "match_category": candidate.get("match_category") or score_category(float(candidate.get("confidence") or 0)),
            "match_reason": candidate.get("match_reason") or "Likely table from similar fields",
            "best_columns": [candidate],
            "row_multiplication_risk": bool(candidate.get("row_multiplication_risk")),
        }
    return sorted(by_view.values(), key=lambda item: (float(item.get("confidence") or 0), str(item.get("source_view") or "")), reverse=True)[:5]


def smart_match_debug_log(event: str, **payload: Any) -> None:
    print(safe_json_dumps({"event": event, **payload}, sort_keys=True), flush=True)


def start_smart_match_heartbeat(state: dict[str, Any], stop_event: threading.Event) -> threading.Thread:
    def heartbeat() -> None:
        while not stop_event.wait(1):
            smart_match_debug_log(
                "report_smart_match_heartbeat",
                message="Smart Match still running...",
                elapsed_ms=round((time.perf_counter() - float(state.get("started_at") or time.perf_counter())) * 1000),
                current_field=state.get("current_field"),
                current_table=state.get("current_table"),
                current_candidate=state.get("current_candidate"),
                current_iteration=state.get("current_iteration"),
                current_candidate_count=state.get("current_candidate_count"),
                stage=state.get("stage"),
            )

    thread = threading.Thread(target=heartbeat, name="smart-match-heartbeat", daemon=True)
    thread.start()
    return thread


def build_candidate_index(views: list[dict[str, Any]]) -> dict[str, Any]:
    entries = []
    token_index: dict[str, list[dict[str, Any]]] = {}
    for view in views:
        for column in view.get("column_index") or view.get("columns") or []:
            column_name = str(column.get("source_column") if isinstance(column, dict) else column)
            if not column_name:
                continue
            base_name = strip_hash_suffix(column_name)
            tokens = set(column.get("tokens") or expanded_tokens(base_name)) if isinstance(column, dict) else expanded_tokens(base_name)
            raw_tokens = set(column.get("raw_tokens") or split_name_tokens(base_name)) if isinstance(column, dict) else set(split_name_tokens(base_name))
            aliases = set(column.get("aliases") or []) if isinstance(column, dict) else alias_norms_for(column_name)
            search_terms = {
                normalize_field(column_name),
                normalize_field(base_name),
                normalize_core_field(base_name),
                normalize_compact(column_name),
                normalize_compact(base_name),
                *tokens,
                *raw_tokens,
                *aliases,
            }
            entry = {
                "view": view,
                "column": column,
                "source_view": view.get("view_name"),
                "source_column": column_name,
                "search_terms": {term for term in search_terms if term},
                "tokens": tokens,
                "raw_tokens": raw_tokens,
            }
            entries.append(entry)
            for token in entry["search_terms"]:
                token_index.setdefault(token, []).append(entry)
    return {"entries": entries, "token_index": token_index, "view_count": len(views), "column_count": len(entries)}


def prefilter_candidate_entries(field_name: str, candidate_index: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    field_tokens = expanded_tokens(field_name)
    raw_tokens = set(split_name_tokens(field_name))
    field_norm = normalize_field(field_name)
    field_core = normalize_core_field(field_name)
    compact = normalize_compact(field_name)
    alias_norms = alias_norms_for(field_name)
    alias_compacts = {normalize_compact(alias) for alias in SEMANTIC_ALIASES.get(semantic_key(field_name), ())}
    search_terms = {field_norm, field_core, compact, *field_tokens, *raw_tokens, *alias_norms, *alias_compacts}
    token_index = candidate_index.get("token_index") or {}
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for term in search_terms:
        if not term:
            continue
        for entry in token_index.get(term, []):
            by_key[(str(entry.get("source_view")), str(entry.get("source_column")))] = entry

    def cheap_relevance(entry: dict[str, Any]) -> float:
        terms = entry.get("search_terms") or set()
        tokens = entry.get("tokens") or set()
        raw = entry.get("raw_tokens") or set()
        score = 0.0
        if field_norm in terms:
            score += 10
        if field_core in terms:
            score += 8
        if compact in terms:
            score += 8
        score += len(alias_norms.intersection(terms)) * 7
        score += len(alias_compacts.intersection(terms)) * 7
        score += len(field_tokens.intersection(tokens)) * 3
        score += len(raw_tokens.intersection(raw)) * 2
        source_column = str(entry.get("source_column") or "")
        if source_column and (field_norm in normalize_field(source_column) or normalize_field(source_column) in field_norm):
            score += 1.5
        return score

    entries = list(by_key.values())
    if len(entries) < 25:
        for entry in candidate_index.get("entries") or []:
            terms = entry.get("search_terms") or set()
            if (
                field_norm in terms
                or field_core in terms
                or compact in terms
                or alias_norms.intersection(terms)
                or field_tokens.intersection(entry.get("tokens") or set())
                or raw_tokens.intersection(entry.get("raw_tokens") or set())
            ):
                by_key[(str(entry.get("source_view")), str(entry.get("source_column")))] = entry
        entries = list(by_key.values())

    if not entries:
        entries = list(candidate_index.get("entries") or [])
    if len(entries) > SMART_MATCH_MAX_PREFILTERED_CANDIDATES:
        entries = sorted(entries, key=cheap_relevance, reverse=True)[:SMART_MATCH_MAX_PREFILTERED_CANDIDATES]
    return entries, {
        "views_scanned": int(candidate_index.get("view_count") or 0),
        "columns_scanned": len(entries),
        "comparisons": len(entries),
        "total_columns": int(candidate_index.get("column_count") or 0),
    }


def indexed_field_candidates(
    field_name: str,
    candidate_index: dict[str, Any],
    *,
    preferred_view: str | None = None,
    used_pairs: set[tuple[str, str]] | None = None,
    debug_state: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    used_pairs = used_pairs or set()
    entries, stats = prefilter_candidate_entries(field_name, candidate_index)
    if debug_state is not None:
        debug_state.update(
            {
                "stage": "candidate_generation",
                "current_field": field_name,
                "current_candidate_count": len(entries),
                "current_iteration": 0,
            }
        )
    smart_match_debug_log(
        "report_smart_match_candidates",
        field=field_name,
        candidate_count=len(entries),
        view_count=stats.get("views_scanned", 0),
        column_count=stats.get("total_columns", 0),
        columns_scanned=stats.get("columns_scanned", 0),
        comparisons=stats.get("comparisons", 0),
    )
    candidates = []
    fallback_candidates = []
    for iteration, entry in enumerate(entries, 1):
        view = entry["view"]
        column = entry["column"]
        source_column = entry.get("source_column")
        if debug_state is not None:
            debug_state.update(
                {
                    "current_table": view.get("view_name"),
                    "current_candidate": source_column,
                    "current_iteration": iteration,
                    "current_candidate_count": len(entries),
                }
            )
        candidate = score_candidate(
            field_name,
            view,
            column,
            preferred_view=preferred_view,
            duplicate=(view.get("view_name"), source_column) in used_pairs,
        )
        fallback_candidates.append(candidate)
        if candidate["confidence"] >= CANDIDATE_MIN_CONFIDENCE or candidate["match_type"] in STRONG_MATCH_TYPES:
            candidates.append(candidate)
    if not candidates:
        candidates = sorted(fallback_candidates, key=lambda item: float(item.get("confidence") or 0), reverse=True)[:5]
        for candidate in candidates:
            candidate["match_category"] = "Weak Match"
            candidate["warnings"] = list(dict.fromkeys([*(candidate.get("warnings") or []), "below reliable threshold"]))
            candidate["match_reason"] = f"{candidate.get('match_reason') or 'Candidate scored by Smart Match'}; below reliable threshold"
    return sorted(
        candidates,
        key=lambda item: (
            float(item.get("confidence") or 0),
            item["source_view"] == preferred_view,
            match_priority(item.get("match_type")),
            *clean_column_priority(str(item.get("source_column") or "")),
            not item.get("sensitive_warning"),
            not item.get("warnings"),
        ),
        reverse=True,
    )[:50], stats


def rank_safe_views_indexed(
    requested_fields: list[dict[str, Any]],
    views: list[dict[str, Any]],
    candidate_index: dict[str, Any],
    *,
    debug_state: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, dict[str, int]]]:
    template_tokens = expanded_tokens(" ".join(field["field_name"] for field in requested_fields))
    by_field: dict[str, list[dict[str, Any]]] = {}
    stats_by_field: dict[str, dict[str, int]] = {}
    score_by_view: dict[str, float] = {}
    coverage_by_view: dict[str, int] = {}
    exact_by_view: dict[str, int] = {}
    sensitive_by_view: dict[str, int] = {}
    for field in requested_fields:
        field_name = field["field_name"]
        smart_match_debug_log("report_smart_match_field_start", field=field_name, stage="candidate_generation")
        if debug_state is not None:
            debug_state.update({"stage": "candidate_ranking", "current_field": field_name, "current_iteration": 0})
        candidates, stats = indexed_field_candidates(field_name, candidate_index, debug_state=debug_state)
        by_field[field["field_key"]] = candidates
        stats_by_field[field["field_key"]] = stats
        best_by_view: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            view_name = candidate["source_view"]
            if view_name not in best_by_view or candidate["confidence"] > best_by_view[view_name]["confidence"]:
                best_by_view[view_name] = candidate
        for view_name, candidate in best_by_view.items():
            required_weight = 1.12 if field.get("required", True) else 0.86
            score_by_view[view_name] = score_by_view.get(view_name, 0.0) + float(candidate["confidence"]) * required_weight
            coverage_by_view[view_name] = coverage_by_view.get(view_name, 0) + 1
            if candidate.get("match_type") in {"exact", "normalized", "business_alias", "historical"}:
                exact_by_view[view_name] = exact_by_view.get(view_name, 0) + 1
            if candidate.get("sensitive_warning"):
                sensitive_by_view[view_name] = sensitive_by_view.get(view_name, 0) + 1
        smart_match_debug_log(
            "report_smart_match_field_end",
            field=field_name,
            stage="candidate_generation",
            candidate_count=len(candidates),
            view_count=stats.get("views_scanned", 0),
            column_count=stats.get("total_columns", 0),
            columns_scanned=stats.get("columns_scanned", 0),
            comparisons=stats.get("comparisons", 0),
        )
    ranked = []
    total_required = max(1, len([field for field in requested_fields if field.get("required", True)]))
    for view in views:
        view_name = view.get("view_name")
        if not view_name:
            continue
        view_tokens = set(view.get("tokens") or expanded_tokens(f"{view.get('view_name')} {view.get('source_table')} {view.get('collection_name')} {view.get('database_name')}"))
        table_relevance = token_similarity(template_tokens, view_tokens)
        coverage = coverage_by_view.get(view_name, 0)
        coherence_bonus = (coverage / total_required) * 1.25
        exact_bonus = exact_by_view.get(view_name, 0) * 0.22
        sensitivity_penalty = sensitive_by_view.get(view_name, 0) * 0.08
        row_bonus = 0.04 if int(view.get("row_count") or 0) > 0 else 0.0
        score = score_by_view.get(view_name, 0.0) + (table_relevance * 1.6) + coherence_bonus + exact_bonus + row_bonus - sensitivity_penalty
        if coverage:
            ranked.append(
                {
                    "source_view": view_name,
                    "source_table": view.get("source_table"),
                    "source_kind": view.get("source_kind"),
                    "database_name": view.get("database_name"),
                    "collection_name": view.get("collection_name"),
                    "row_count": view.get("row_count"),
                    "score": round(score, 3),
                    "matched_fields": coverage,
                    "coverage": round(coverage / max(1, len(requested_fields)), 3),
                    "table_relevance": round(table_relevance, 3),
                    "reason": "dominant safe view" if coverage >= max(2, total_required // 2) else "partial table match",
                }
            )
    return sorted(ranked, key=lambda item: (item["score"], item["matched_fields"], item["coverage"]), reverse=True), by_field, stats_by_field


def field_candidates(field_name: str, views: list[dict[str, Any]], *, preferred_view: str | None = None, used_pairs: set[tuple[str, str]] | None = None) -> list[dict[str, Any]]:
    used_pairs = used_pairs or set()
    candidates = []
    fallback_candidates = []
    for view in views:
        for column in view.get("column_index") or view.get("columns") or []:
            candidate = score_candidate(
                field_name,
                view,
                column,
                preferred_view=preferred_view,
                duplicate=(view.get("view_name"), column.get("source_column") if isinstance(column, dict) else str(column)) in used_pairs,
            )
            fallback_candidates.append(candidate)
            if candidate["confidence"] >= CANDIDATE_MIN_CONFIDENCE or candidate["match_type"] in STRONG_MATCH_TYPES:
                candidates.append(candidate)
    if not candidates:
        candidates = sorted(fallback_candidates, key=lambda item: float(item.get("confidence") or 0), reverse=True)[:5]
        for candidate in candidates:
            candidate["match_category"] = "Weak Match"
            candidate["warnings"] = list(dict.fromkeys([*(candidate.get("warnings") or []), "below reliable threshold"]))
            candidate["match_reason"] = f"{candidate.get('match_reason') or 'Candidate scored by Smart Match'}; below reliable threshold"
    return sorted(
        candidates,
        key=lambda item: (
            float(item.get("confidence") or 0),
            item["source_view"] == preferred_view,
            match_priority(item.get("match_type")),
            *clean_column_priority(str(item.get("source_column") or "")),
            not item.get("sensitive_warning"),
            not item.get("warnings"),
        ),
        reverse=True,
    )[:50]


def candidate_for_view(
    field_name: str,
    source_view: str,
    views: list[dict[str, Any]],
    *,
    candidates: list[dict[str, Any]] | None = None,
    used_pairs: set[tuple[str, str]] | None = None,
    minimum_confidence: float = REPORT_AUTO_FIX_MIN_CONFIDENCE,
) -> dict[str, Any] | None:
    used_pairs = used_pairs or set()
    view_candidates = [
        candidate
        for candidate in candidates or []
        if candidate.get("source_view") == source_view
        and candidate.get("source_column")
        and (candidate.get("source_view"), candidate.get("source_column")) not in used_pairs
    ]
    if not view_candidates:
        view_candidates = [
            candidate
            for candidate in field_candidates(field_name, views, preferred_view=source_view, used_pairs=used_pairs)
            if candidate.get("source_view") == source_view
            and candidate.get("source_column")
            and (candidate.get("source_view"), candidate.get("source_column")) not in used_pairs
        ]
    view_candidates = sorted(
        view_candidates,
        key=lambda item: (
            float(item.get("confidence") or 0),
            match_priority(item.get("match_type")),
            *clean_column_priority(str(item.get("source_column") or "")),
        ),
        reverse=True,
    )
    for candidate in view_candidates:
        if float(candidate.get("confidence") or 0) >= minimum_confidence:
            return candidate
    return None


def rank_safe_views(requested_fields: list[dict[str, Any]], views: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    template_tokens = expanded_tokens(" ".join(field["field_name"] for field in requested_fields))
    by_field: dict[str, list[dict[str, Any]]] = {}
    score_by_view: dict[str, float] = {}
    coverage_by_view: dict[str, int] = {}
    exact_by_view: dict[str, int] = {}
    sensitive_by_view: dict[str, int] = {}
    for field in requested_fields:
        field_name = field["field_name"]
        candidates = field_candidates(field_name, views)
        by_field[field["field_key"]] = candidates
        best_by_view: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            view_name = candidate["source_view"]
            if view_name not in best_by_view or candidate["confidence"] > best_by_view[view_name]["confidence"]:
                best_by_view[view_name] = candidate
        for view_name, candidate in best_by_view.items():
            required_weight = 1.12 if field.get("required", True) else 0.86
            score_by_view[view_name] = score_by_view.get(view_name, 0.0) + float(candidate["confidence"]) * required_weight
            coverage_by_view[view_name] = coverage_by_view.get(view_name, 0) + 1
            if candidate.get("match_type") in {"exact", "normalized", "business_alias", "historical"}:
                exact_by_view[view_name] = exact_by_view.get(view_name, 0) + 1
            if candidate.get("sensitive_warning"):
                sensitive_by_view[view_name] = sensitive_by_view.get(view_name, 0) + 1
    ranked = []
    total_required = max(1, len([field for field in requested_fields if field.get("required", True)]))
    for view in views:
        view_name = view.get("view_name")
        if not view_name:
            continue
        view_tokens = set(view.get("tokens") or expanded_tokens(f"{view.get('view_name')} {view.get('source_table')} {view.get('collection_name')} {view.get('database_name')}"))
        table_relevance = token_similarity(template_tokens, view_tokens)
        coverage = coverage_by_view.get(view_name, 0)
        coherence_bonus = (coverage / total_required) * 1.25
        exact_bonus = exact_by_view.get(view_name, 0) * 0.22
        sensitivity_penalty = sensitive_by_view.get(view_name, 0) * 0.08
        row_bonus = 0.04 if int(view.get("row_count") or 0) > 0 else 0.0
        score = score_by_view.get(view_name, 0.0) + (table_relevance * 1.6) + coherence_bonus + exact_bonus + row_bonus - sensitivity_penalty
        if coverage:
            ranked.append(
                {
                    "source_view": view_name,
                    "source_table": view.get("source_table"),
                    "source_kind": view.get("source_kind"),
                    "database_name": view.get("database_name"),
                    "collection_name": view.get("collection_name"),
                    "row_count": view.get("row_count"),
                    "score": round(score, 3),
                    "matched_fields": coverage,
                    "coverage": round(coverage / max(1, len(requested_fields)), 3),
                    "table_relevance": round(table_relevance, 3),
                    "reason": "dominant safe view" if coverage >= max(2, total_required // 2) else "partial table match",
                }
            )
    return sorted(ranked, key=lambda item: (item["score"], item["matched_fields"], item["coverage"]), reverse=True), by_field


def match_summary(matches: list[dict[str, Any]], primary_view: str | None, metadata: dict[str, Any]) -> dict[str, Any]:
    required_matches = [match for match in matches if match.get("required", True) and not match.get("intentionally_unmapped")]
    mapped = [match for match in matches if match.get("source_view") and match.get("source_column")]
    unmatched = [match for match in matches if match.get("required", True) and not match.get("intentionally_unmapped") and not (match.get("source_view") and match.get("source_column"))]
    ambiguous = [match for match in matches if match.get("field_status") in {"possible_match", "weak_match", "ambiguous", "needs_review"}]
    warnings = []
    for match in matches:
        if match.get("sensitive_warning"):
            warnings.append({"field_name": match.get("field_name"), "type": "sensitive_field", "message": match.get("sensitive_warning")})
        for warning in match.get("warnings") or []:
            warnings.append({"field_name": match.get("field_name"), "type": "match_warning", "message": warning})
    if metadata.get("missing_safe_views"):
        warnings.append({"type": "missing_safe_views", "message": "Some expected safe views are missing.", "views": metadata.get("missing_safe_views")})
    denominator = max(1, len(required_matches))
    overall = sum(float(match.get("confidence") or 0) for match in required_matches) / denominator
    fix_actions = []
    if ambiguous:
        fix_actions.append({"action": "confirm_ambiguous_fields", "message": "Review ambiguous fields and select one of the top candidates."})
    if unmatched:
        fix_actions.append({"action": "map_or_mark_unmatched_fields", "message": "Map unmatched required fields or mark them intentionally unmapped."})
    cross_view = sorted({match.get("source_view") for match in mapped if match.get("source_view")})
    if primary_view and len(cross_view) > 1:
        fix_actions.append({"action": "prefer_selected_best_view", "primary_source_view": primary_view, "message": "Use the dominant safe view where possible to keep preview simple."})
    return {
        "selected_best_view": primary_view,
        "mapped_fields": [{"field_key": match.get("field_key"), "field_name": match.get("field_name"), "source_view": match.get("source_view"), "source_column": match.get("source_column"), "confidence": match.get("confidence")} for match in mapped],
        "unmatched_fields": [{"field_key": match.get("field_key"), "field_name": match.get("field_name"), "required": match.get("required")} for match in unmatched],
        "ambiguous_fields": [{"field_key": match.get("field_key"), "field_name": match.get("field_name"), "candidates": (match.get("candidates") or [])[:3]} for match in ambiguous],
        "warnings": warnings,
        "overall_confidence": round(overall, 3),
        "suggested_fix_actions": fix_actions,
    }


def smart_match_fields(
    fields: list[Any] | None = None,
    existing_matches: list[dict[str, Any]] | None = None,
    *,
    use_cached_metadata: bool = False,
    cached_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    debug_state: dict[str, Any] = {
        "started_at": started_at,
        "stage": "start",
        "current_field": None,
        "current_table": None,
        "current_candidate": None,
        "current_iteration": 0,
        "current_candidate_count": 0,
    }
    heartbeat_stop = threading.Event()
    start_smart_match_heartbeat(debug_state, heartbeat_stop)
    timings: dict[str, int] = {}
    try:
        smart_match_debug_log("report_smart_match_start", field_count=len(fields or []), existing_match_count=len(existing_matches or []))
        debug_state["stage"] = "parse_template"
        stage_started = time.perf_counter()
        requested_fields = normalize_requested_fields(fields)
        timings["parse_template"] = round((time.perf_counter() - stage_started) * 1000)

        debug_state["stage"] = "field_normalization"
        stage_started = time.perf_counter()
        normalized_field_debug = [
            {
                "field_key": field.get("field_key"),
                "field_name": field.get("field_name"),
                "normalized": normalize_field(str(field.get("field_name") or "")),
                "tokens": sorted(expanded_tokens(str(field.get("field_name") or ""))),
            }
            for field in requested_fields
        ]
        timings["field_normalization"] = round((time.perf_counter() - stage_started) * 1000)

        debug_state["stage"] = "load_metadata"
        stage_started = time.perf_counter()
        metadata = cached_metadata_layer_source_metadata(
            require_cached=use_cached_metadata,
            metadata_override=cached_metadata,
        )
        timings["load_metadata"] = round((time.perf_counter() - stage_started) * 1000)

        debug_state["stage"] = "load_safe_views"
        stage_started = time.perf_counter()
        views = metadata["views"]
        timings["load_safe_views"] = round((time.perf_counter() - stage_started) * 1000)

        debug_state["stage"] = "build_candidate_index"
        stage_started = time.perf_counter()
        candidate_index = build_candidate_index(views)
        timings["build_candidate_index"] = round((time.perf_counter() - stage_started) * 1000)
        smart_match_debug_log(
            "report_smart_match_index_built",
            metadata_rows_loaded=int(metadata.get("metadata_rows_loaded") or 0),
            safe_views_loaded=len(views),
            columns_indexed=int(candidate_index.get("column_count") or 0),
            trino_calls_count=0,
            superset_calls_count=0,
        )

        existing_by_key = {str(match.get("field_key") or ""): match for match in existing_matches or []}
        existing_by_name = {str(match.get("field_name") or "").strip().lower(): match for match in existing_matches or []}
        debug_state["stage"] = "candidate_ranking"
        stage_started = time.perf_counter()
        ranked_views, first_pass_candidates, stats_by_field = rank_safe_views_indexed(requested_fields, views, candidate_index, debug_state=debug_state)
        timings["candidate_ranking"] = round((time.perf_counter() - stage_started) * 1000)

        primary_view = ranked_views[0]["source_view"] if ranked_views else None
        matches = []
        debug_fields = []
        used_source_pairs: set[tuple[str, str]] = set()
        debug_state["stage"] = "field_matching"
        stage_started = time.perf_counter()
        for field in requested_fields:
            field_started = time.perf_counter()
            field_name = field["field_name"]
            smart_match_debug_log("report_smart_match_field_start", field=field_name, stage="field_matching")
            debug_state.update({"stage": "field_matching", "current_field": field_name, "current_iteration": 0})
            existing = existing_by_key.get(field["field_key"]) or existing_by_name.get(field_name.strip().lower()) or {}
            cached_candidates = first_pass_candidates.get(field["field_key"], [])
            table_candidates = table_candidates_from_field_candidates(cached_candidates)
            selected: dict[str, Any] | None = None
            status = "missing"
            if not views:
                candidates = []
                status = "missing_safe_views"
            elif field.get("intentionally_unmapped") and not (existing.get("source_view") and existing.get("source_column")):
                candidates = cached_candidates
                status = "intentionally_unmapped"
            elif existing.get("manual_override") and (existing.get("intentionally_unmapped") or (existing.get("source_view") and existing.get("source_column"))):
                candidates = cached_candidates or field_candidates(field_name, views, preferred_view=primary_view, used_pairs=used_source_pairs)
                selected = {
                    **existing,
                    "confidence": float(existing.get("confidence") or 1),
                    "match_reason": existing.get("match_reason") or "Manual override remembered for this template/session",
                    "match_type": existing.get("match_type") or "manual",
                    "match_category": "Manual",
                }
                status = "intentionally_unmapped" if existing.get("intentionally_unmapped") else "manually_selected"
            else:
                candidates = cached_candidates or field_candidates(field_name, views, preferred_view=primary_view, used_pairs=used_source_pairs)
                ordered = sorted(
                    candidates,
                    key=lambda item: (
                        float(item.get("confidence") or 0),
                        item.get("source_view") == primary_view,
                        match_priority(item.get("match_type")),
                        *clean_column_priority(str(item.get("source_column") or "")),
                    ),
                    reverse=True,
                )
                selection_top = ordered[0] if ordered else None
                if selection_top:
                    selected = selection_top
                    selection_confidence = float(selection_top.get("confidence") or 0)
                    if selection_confidence >= 0.95:
                        status = "excellent_match"
                    elif selection_confidence >= 0.80:
                        status = "good_match"
                    elif selection_confidence >= 0.60:
                        status = "possible_match"
                    else:
                        status = "weak_match"
                elif candidates:
                    status = "missing"
                if selected and selected.get("source_view") and selected.get("source_column"):
                    selected_key = (selected["source_view"], selected["source_column"])
                    if selected_key in used_source_pairs:
                        replacement = next(
                            (
                                candidate
                                for candidate in ordered
                                if (candidate.get("source_view"), candidate.get("source_column")) not in used_source_pairs
                                and candidate.get("source_view") == selected.get("source_view")
                                and candidate.get("confidence", 0) >= NEEDS_REVIEW_CONFIDENCE
                            ),
                            None,
                        )
                        if replacement:
                            selected = {**replacement, "match_reason": f"{replacement.get('match_reason')}; duplicate candidate avoided"}
                        else:
                            selected = None
                            status = "ambiguous"
                    if selected and selected.get("source_view") and selected.get("source_column"):
                        used_source_pairs.add((selected["source_view"], selected["source_column"]))

            selected_warnings = list(selected.get("warnings") or []) if selected else []
            match_candidates = candidates[:10]
            for rank, candidate in enumerate(match_candidates, 1):
                candidate["rank"] = rank
                candidate["recommended"] = bool(rank == 1)
                candidate["same_safe_view"] = bool(primary_view and candidate.get("source_view") == primary_view)
                candidate["match_category"] = candidate.get("match_category") or score_category(float(candidate.get("confidence") or 0))
                candidate["confidence_percent"] = round(float(candidate.get("confidence") or 0) * 100)
            field_stats = stats_by_field.get(field["field_key"], {})
            field_debug = {
                "field": field_name,
                "views_scanned": field_stats.get("views_scanned", 0),
                "columns_scanned": field_stats.get("columns_scanned", 0),
                "comparisons": field_stats.get("comparisons", 0),
                "total_columns": field_stats.get("total_columns", 0),
                "candidate_count": len(cached_candidates),
                "top_match": selected.get("source_column") if selected else None,
                "top_view": selected.get("source_view") if selected else None,
                "confidence": round(float(selected.get("confidence") or 0) * 100) if selected else 0,
                "time_ms": round((time.perf_counter() - field_started) * 1000),
            }
            debug_fields.append(field_debug)
            selected_confidence = float(selected.get("confidence") or 0) if selected else 0.0
            missing = not selected
            matches.append(
                {
                    "field_key": field["field_key"],
                    "field_name": field_name,
                    "required": bool(field.get("required", True)),
                    "source_view": selected.get("source_view") if selected else None,
                    "source_column": selected.get("source_column") if selected else None,
                    "confidence": selected_confidence,
                    "confidence_percent": round(selected_confidence * 100),
                    "match_category": score_category(selected_confidence, selected=bool(selected), missing=missing),
                    "match_reason": selected.get("match_reason") if selected else metadata["message"] if not metadata["views"] else "No reliable match found",
                    "match_type": selected.get("match_type") if selected else "missing",
                    "field_status": status,
                    "sensitive_warning": selected.get("sensitive_warning") if selected else sensitive_warning_for(field_name, None),
                    "warnings": selected_warnings,
                    "candidates": match_candidates,
                    "table_candidates": table_candidates,
                    "intentionally_unmapped": field.get("intentionally_unmapped", False),
                    "manual_override": bool(existing.get("manual_override")) if selected and existing.get("manual_override") else False,
                    "ambiguous": status == "ambiguous",
                }
            )
            smart_match_debug_log("report_smart_match_field_end", stage="field_matching", **field_debug)

        timings["field_matching"] = round((time.perf_counter() - stage_started) * 1000)
        timings["candidate_search"] = timings["candidate_ranking"]
        timings["scoring"] = timings["candidate_ranking"]
        timings["ranking"] = timings["candidate_ranking"]
        matching_time_ms = timings["build_candidate_index"] + timings["candidate_ranking"] + timings["field_matching"]
        summary = match_summary(matches, primary_view, metadata)
        total_duration_ms = round((time.perf_counter() - started_at) * 1000)
        timings["total"] = total_duration_ms
        evidence = {
            "metadata_rows_loaded": int(metadata.get("metadata_rows_loaded") or 0),
            "safe_views_loaded": len(views),
            "columns_indexed": int(candidate_index.get("column_count") or 0),
            "trino_calls_count": 0,
            "superset_calls_count": 0,
            "matching_time_ms": matching_time_ms,
            "total_time_ms": total_duration_ms,
        }
        smart_match_debug_log(
            "report_smart_match_timing",
            field_count=len(matches),
            usable_safe_views=len(views),
            timings_ms=timings,
            cache_status=metadata.get("cache_status"),
            candidate_index={"views": candidate_index.get("view_count"), "columns": candidate_index.get("column_count")},
            field_debug=debug_fields,
            **evidence,
        )
        debug_state["stage"] = "audit_event"
        audit_event(
            None,
            "smart_match_run",
            "Smart match run completed",
            {
                "field_count": len(matches),
                "usable_safe_views": len(views),
                "primary_source_view": primary_view,
                "duration_ms": total_duration_ms,
                **evidence,
                "timings_ms": timings,
                "field_debug": debug_fields,
                "cache_status": metadata.get("cache_status"),
            },
        )
        debug_state["stage"] = "response_build"
        return {
            "status": metadata["status"],
            "message": metadata["message"],
            "source": metadata["source"],
            "matches": matches,
            "source_views": metadata["views"],
            "ranked_source_views": ranked_views,
            "missing_safe_views": metadata.get("missing_safe_views", []),
            "primary_source_view": primary_view,
            "selected_best_view": primary_view,
            "mapped_fields": summary["mapped_fields"],
            "unmatched_fields": summary["unmatched_fields"],
            "ambiguous_fields": summary["ambiguous_fields"],
            "warnings": summary["warnings"],
            "overall_confidence": summary["overall_confidence"],
            "suggested_fix_actions": summary["suggested_fix_actions"],
            "metadata_cache_status": metadata.get("cache_status"),
            "metadata_source": metadata.get("source"),
            **evidence,
            "duration_ms": total_duration_ms,
            "timings_ms": timings,
            "field_debug": debug_fields,
            "normalized_fields": normalized_field_debug,
            "candidate_index": {"views": candidate_index.get("view_count"), "columns": candidate_index.get("column_count")},
            "skipped_safe_views": metadata.get("skipped_safe_views", []),
            "incomplete_metadata_views": metadata.get("incomplete_metadata_views", []),
        }
    except Exception as exc:
        smart_match_debug_log(
            "report_smart_match_failed",
            error_type=exc.__class__.__name__,
            error_message=str(exc),
            elapsed_ms=round((time.perf_counter() - started_at) * 1000),
            current_field=debug_state.get("current_field"),
            current_table=debug_state.get("current_table"),
            current_candidate=debug_state.get("current_candidate"),
            current_iteration=debug_state.get("current_iteration"),
            stage=debug_state.get("stage"),
        )
        raise
    finally:
        heartbeat_stop.set()


def upsert_template(template_name: str, matches: list[dict[str, Any]], requested_fields: list[Any] | None = None, generated_form: dict[str, Any] | None = None) -> dict[str, Any]:
    init_report_template_db()
    name = template_name.strip()
    if not name:
        raise ValueError("template_name is required")
    fields = normalize_requested_fields(requested_fields, matches)
    if not fields:
        raise ValueError("At least one requested report field is required")
    match_by_key = {str(match.get("field_key") or ""): match for match in matches}
    match_by_name = {str(match.get("field_name") or ""): match for match in matches}
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM report_templates WHERE template_name = %s", (name,))
            existing = as_dict(cursor.fetchone())
            cursor.execute(
                """
                INSERT INTO report_templates (id, template_name, status, generated_form_json, updated_at)
                VALUES (%s, %s, 'draft', %s, now())
                ON CONFLICT (template_name)
                DO UPDATE SET
                    status = CASE
                        WHEN report_templates.generation_status = 'generated' THEN 'needs_regeneration'
                        ELSE 'mapping_in_progress'
                    END,
                    generation_status = CASE
                        WHEN report_templates.generation_status = 'generated' THEN 'needs_regeneration'
                        ELSE report_templates.generation_status
                    END,
                    generated_form_json = EXCLUDED.generated_form_json,
                    updated_at = now()
                RETURNING *
                """,
                (new_id(), name, Json(json_safe(generated_form or {}))),
            )
            template = as_dict(cursor.fetchone())
            template_id = template["id"]
            cursor.execute("DELETE FROM report_template_fields WHERE template_id = %s", (template_id,))
            cursor.execute("DELETE FROM report_field_matches WHERE template_id = %s", (template_id,))
            for field in fields:
                field_name = field["field_name"]
                field_key = field["field_key"]
                cursor.execute(
                    """
                    INSERT INTO report_template_fields (
                        id, template_id, field_key, field_name, display_order, required, intentionally_unmapped
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        new_id(),
                        template_id,
                        field_key,
                        field_name,
                        field["display_order"],
                        bool(field.get("required", True)),
                        bool(field.get("intentionally_unmapped")),
                    ),
                )
                match = match_by_key.get(field_key) or match_by_name.get(field_name) or {}
                cursor.execute(
                    """
                    INSERT INTO report_field_matches (
                        id, template_id, field_key, field_name, source_view, source_column, confidence,
                        match_reason, match_type, field_status, sensitive_warning,
                        candidates_json, manual_override, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    """,
                    (
                        new_id(),
                        template_id,
                        field_key,
                        field_name,
                        match.get("source_view"),
                        match.get("source_column"),
                        float(match.get("confidence") or 0),
                        match.get("match_reason"),
                        match.get("match_type"),
                        match.get("field_status") or "not_matched",
                        match.get("sensitive_warning"),
                        Json(json_safe(match.get("candidates") or [])),
                        bool(match.get("manual_override")),
                    ),
                )
    audit_event(str(template_id), "template_updated" if existing else "template_created", f"Template {name} saved", {"field_count": len(fields)})
    if any(match.get("manual_override") for match in matches):
        audit_event(
            str(template_id),
            "manual_mapping_changed",
            "Manual report field mapping saved",
            {"manual_mapping_count": len([match for match in matches if match.get("manual_override")])},
        )
    return get_template(str(template_id))


def list_templates() -> list[dict[str, Any]]:
    init_report_template_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM report_templates ORDER BY updated_at DESC")
            return [as_dict(row) for row in cursor.fetchall()]


def get_template(template_id: str) -> dict[str, Any]:
    init_report_template_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM report_templates WHERE id = %s", (template_id,))
            template = as_dict(cursor.fetchone())
            if not template:
                raise ValueError("Template not found")
            cursor.execute(
                """
                SELECT f.field_key, f.field_name, f.display_order, f.required, f.intentionally_unmapped,
                       m.source_view, m.source_column, m.confidence, m.match_reason,
                       m.match_type, m.field_status, m.sensitive_warning,
                       m.candidates_json, m.manual_override
                FROM report_template_fields f
                LEFT JOIN report_field_matches m
                  ON m.template_id = f.template_id AND m.field_key = f.field_key
                WHERE f.template_id = %s
                ORDER BY f.display_order, f.field_name
                """,
                (template_id,),
            )
            matches = []
            for row in cursor.fetchall():
                item = as_dict(row)
                item["candidates"] = list(item.pop("candidates_json") or [])
                matches.append(item)
            cursor.execute(
                "SELECT * FROM report_generation_runs WHERE template_id = %s ORDER BY created_at DESC LIMIT 10",
                (template_id,),
            )
            runs = [as_dict(row) for row in cursor.fetchall()]
    generated_form = template.pop("generated_form_json", None) or {}
    return {**template, "generated_form": generated_form, "matches": matches, "generation_runs": runs}


def selected_matches(template: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        match
        for match in template.get("matches") or []
        if match.get("source_view") and match.get("source_column") and not match.get("intentionally_unmapped")
    ]


def output_fields(template: dict[str, Any]) -> list[dict[str, Any]]:
    fields = []
    for match in template.get("matches") or []:
        if match.get("source_view") and match.get("source_column"):
            fields.append({**match, "output_kind": "mapped"})
    return fields


def single_view_repair_plan(template: dict[str, Any], preferred_view: str | None = None) -> dict[str, Any] | None:
    mapped = selected_matches(template)
    if not mapped:
        return None
    source_views = sorted({match["source_view"] for match in mapped})
    if not source_views:
        return None
    by_view: dict[str, list[dict[str, Any]]] = {}
    for match in mapped:
        by_view.setdefault(match["source_view"], []).append(match)
    if preferred_view and preferred_view in by_view:
        primary_view = preferred_view
    else:
        primary_view = max(
            by_view,
            key=lambda view_name: (
                len(by_view[view_name]),
                sum(float(match.get("confidence") or 0) for match in by_view[view_name]),
                0 if is_child_view(view_name) else 1,
            ),
        )
    kept = by_view.get(primary_view, [])
    removed = [match for match in mapped if match.get("source_view") != primary_view]
    return {
        "action": "use_best_single_view_mapping",
        "primary_source_view": primary_view,
        "chosen_primary_view": primary_view,
        "original_selected_views": source_views,
        "kept_field_keys": [match.get("field_key") for match in kept],
        "kept_fields": [match.get("field_name") for match in kept],
        "rematch_field_keys": [match.get("field_key") for match in removed],
        "rematch_fields": [match.get("field_name") for match in removed],
        "message": (
            f"Keep {len(kept)} mapped field(s) from {primary_view} and rematch or mark "
            f"{len(removed)} cross-view field(s) as optional for this v1 flat report."
        ),
    }


def source_view_field_groups(mapped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for match in mapped:
        groups.setdefault(match["source_view"], []).append(match)
    return [
        {
            "source_view": source_view,
            "field_count": len(items),
            "fields": [
                {
                    "field_key": item.get("field_key"),
                    "field_name": item.get("field_name"),
                    "source_column": item.get("source_column"),
                    "confidence": item.get("confidence"),
                }
                for item in items
            ],
        }
        for source_view, items in sorted(groups.items(), key=lambda pair: (-len(pair[1]), pair[0]))
    ]


def validation_step(name: str, ok: bool, message: str, *, detail: str | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "status": "passed" if ok else "failed",
        "message": message,
        "detail": detail,
    }


def unmatched_fields(template: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "field_key": match.get("field_key"),
            "field_name": match.get("field_name"),
            "required": bool(match.get("required")),
            "intentionally_unmapped": bool(match.get("intentionally_unmapped")),
            "field_status": match.get("field_status"),
        }
        for match in template.get("matches") or []
        if not (match.get("source_view") and match.get("source_column"))
    ]


def _update_match_from_candidate(cursor: Any, template_id: str, field_key: str, candidate: dict[str, Any], reason: str) -> None:
    cursor.execute(
        """
        UPDATE report_template_fields
        SET intentionally_unmapped = false
        WHERE template_id = %s AND field_key = %s
        """,
        (template_id, field_key),
    )
    cursor.execute(
        """
        UPDATE report_field_matches
        SET source_view = %s,
            source_column = %s,
            confidence = %s,
            match_reason = %s,
            match_type = %s,
            field_status = 'auto_fixed',
            sensitive_warning = %s,
            manual_override = false,
            updated_at = now()
        WHERE template_id = %s AND field_key = %s
        """,
        (
            candidate.get("source_view"),
            candidate.get("source_column"),
            float(candidate.get("confidence") or 0),
            reason,
            candidate.get("match_type") or "auto_fixed",
            candidate.get("sensitive_warning"),
            template_id,
            field_key,
        ),
    )


def _mark_match_optional_unmatched(cursor: Any, template_id: str, field_key: str, reason: str) -> None:
    cursor.execute(
        """
        UPDATE report_template_fields
        SET intentionally_unmapped = true,
            required = false
        WHERE template_id = %s AND field_key = %s
        """,
        (template_id, field_key),
    )
    cursor.execute(
        """
        UPDATE report_field_matches
        SET source_view = NULL,
            source_column = NULL,
            confidence = 0,
            match_reason = %s,
            match_type = 'unmatched_optional',
            field_status = 'optional_unmatched',
            manual_override = false,
            updated_at = now()
        WHERE template_id = %s AND field_key = %s
        """,
        (reason, template_id, field_key),
    )


def scoped_views_for_template(template: dict[str, Any], metadata: dict[str, Any], preferred_view: str | None = None) -> list[dict[str, Any]]:
    views = metadata.get("views") or []
    by_name = {view.get("view_name"): view for view in views if view.get("view_name")}
    selected = [match["source_view"] for match in selected_matches(template) if match.get("source_view")]
    selected_names = list(dict.fromkeys([*(preferred_view and [preferred_view] or []), *selected]))
    candidate_names: list[str] = []
    for match in template.get("matches") or []:
        for candidate in match.get("candidates") or []:
            if candidate.get("source_view"):
                candidate_names.append(candidate["source_view"])
    selected_contexts = {
        (view.get("database_name"), view.get("collection_name"))
        for name in selected_names
        for view in [by_name.get(name)]
        if view
    }
    same_database = [
        view.get("view_name")
        for view in views
        if view.get("view_name")
        and any(view.get("database_name") == database for database, _ in selected_contexts if database)
    ]
    ordered_names = [*selected_names, *candidate_names, *same_database, *[view.get("view_name") for view in views]]
    return [by_name[name] for name in dict.fromkeys(name for name in ordered_names if name in by_name)]


def best_saved_candidate(
    match: dict[str, Any],
    *,
    allowed_views: set[str] | None = None,
    used_pairs: set[tuple[str, str]] | None = None,
    minimum_confidence: float = REPORT_AUTO_FIX_MIN_CONFIDENCE,
) -> dict[str, Any] | None:
    used_pairs = used_pairs or set()
    candidates = sorted(
        [
            candidate
            for candidate in match.get("candidates") or []
            if candidate.get("source_view")
            and candidate.get("source_column")
            and (allowed_views is None or candidate.get("source_view") in allowed_views)
            and (candidate.get("source_view"), candidate.get("source_column")) not in used_pairs
        ],
        key=lambda item: (
            float(item.get("confidence") or 0),
            match_priority(item.get("match_type")),
            *clean_column_priority(str(item.get("source_column") or "")),
        ),
        reverse=True,
    )
    for candidate in candidates:
        if float(candidate.get("confidence") or 0) >= minimum_confidence:
            return candidate
    return None


def auto_fix_missing_fields(
    template_id: str,
    *,
    metadata: dict[str, Any] | None = None,
    primary_source_view: str | None = None,
    time_budget_ms: int = 2500,
) -> dict[str, Any] | None:
    started = time.monotonic()
    template = get_template(template_id)
    missing = [
        match
        for match in template.get("matches") or []
        if match.get("required")
        and not match.get("intentionally_unmapped")
        and not (match.get("source_view") and match.get("source_column"))
    ]
    if not missing:
        return None
    metadata = metadata or source_metadata()
    scoped_views = scoped_views_for_template(template, metadata, primary_source_view)
    selected_view_names = {match["source_view"] for match in selected_matches(template) if match.get("source_view")}
    used_pairs = {(match["source_view"], match["source_column"]) for match in selected_matches(template)}
    resolved = []
    unresolved = []
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for match in missing:
                elapsed_ms = timing_ms(started)
                if elapsed_ms >= time_budget_ms:
                    unresolved.append(
                        {
                            "field_key": match.get("field_key"),
                            "field_name": match.get("field_name"),
                            "reason": f"Auto Fix budget exhausted after {elapsed_ms} ms",
                        }
                    )
                    continue
                candidate = (
                    best_saved_candidate(match, allowed_views=selected_view_names or None, used_pairs=used_pairs)
                    or best_saved_candidate(match, used_pairs=used_pairs)
                )
                if not candidate:
                    for view in scoped_views:
                        candidate = candidate_for_view(
                            str(match.get("field_name") or ""),
                            str(view.get("view_name") or ""),
                            [view],
                            candidates=match.get("candidates") or [],
                            used_pairs=used_pairs,
                        )
                        if candidate:
                            break
                if candidate:
                    used_pairs.add((candidate["source_view"], candidate["source_column"]))
                    _update_match_from_candidate(
                        cursor,
                        template_id,
                        str(match.get("field_key")),
                        candidate,
                        f"Auto-fixed missing field from cached/scoped candidates: {candidate['source_view']}.{candidate['source_column']}.",
                    )
                    resolved.append(
                        {
                            "field_key": match.get("field_key"),
                            "field_name": match.get("field_name"),
                            "source_view": candidate.get("source_view"),
                            "source_column": candidate.get("source_column"),
                            "confidence": candidate.get("confidence"),
                        }
                    )
                else:
                    unresolved.append(
                        {
                            "field_key": match.get("field_key"),
                            "field_name": match.get("field_name"),
                            "reason": "No candidate reached confidence threshold",
                        }
                    )
            cursor.execute("UPDATE report_templates SET updated_at = now() WHERE id = %s", (template_id,))
    details = {
        "action": "auto_fix_missing_fields",
        "fields_checked": len(missing),
        "fields_resolved": resolved,
        "fields_unresolved": unresolved,
        "duration_ms": timing_ms(started),
        "search_scope": {
            "selected_views": sorted(selected_view_names),
            "scoped_view_count": len(scoped_views),
            "time_budget_ms": time_budget_ms,
        },
    }
    audit_event(template_id, "missing_field_auto_fix", "Missing report fields auto-fixed where possible", details)
    return details


def auto_fix_duplicate_mappings(template_id: str, metadata: dict[str, Any] | None = None) -> dict[str, Any] | None:
    template = get_template(template_id)
    mapped = selected_matches(template)
    duplicates: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for match in mapped:
        duplicates.setdefault((match["source_view"], match["source_column"]), []).append(match)
    duplicate_groups = {key: items for key, items in duplicates.items() if len(items) > 1}
    if not duplicate_groups:
        return None
    metadata = metadata or source_metadata()
    views = metadata.get("views") or []
    used_pairs = {(match["source_view"], match["source_column"]) for match in mapped}
    rematched = []
    remaining = []
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for (source_view, source_column), items in duplicate_groups.items():
                ordered = sorted(
                    items,
                    key=lambda match: (
                        float(match.get("confidence") or 0),
                        match.get("match_type") in {"manual", "exact", "normalized", "business_alias"},
                    ),
                    reverse=True,
                )
                keeper = ordered[0]
                for match in ordered[1:]:
                    replacement = candidate_for_view(
                        str(match.get("field_name") or ""),
                        source_view,
                        views,
                        candidates=match.get("candidates") or [],
                        used_pairs=used_pairs,
                    )
                    if replacement:
                        used_pairs.add((replacement["source_view"], replacement["source_column"]))
                        _update_match_from_candidate(
                            cursor,
                            template_id,
                            str(match.get("field_key")),
                            replacement,
                            f"Auto-fixed duplicate mapping: rematched from {source_column} to {replacement['source_column']}.",
                        )
                        rematched.append(
                            {
                                "field_key": match.get("field_key"),
                                "field_name": match.get("field_name"),
                                "from_source_column": source_column,
                                "to_source_column": replacement.get("source_column"),
                                "source_view": source_view,
                                "confidence": replacement.get("confidence"),
                            }
                        )
                    else:
                        remaining.append(
                            {
                                "field_key": match.get("field_key"),
                                "field_name": match.get("field_name"),
                                "source_view": source_view,
                                "source_column": source_column,
                                "kept_by": keeper.get("field_name"),
                            }
                        )
            cursor.execute("UPDATE report_templates SET updated_at = now() WHERE id = %s", (template_id,))
    details = {"action": "auto_fix_duplicate_mappings", "fields_rematched": rematched, "remaining_duplicate_warnings": remaining}
    audit_event(template_id, "duplicate_mapping_auto_fix", "Duplicate report mappings auto-fixed where possible", details)
    return details


def apply_single_view_fix(template_id: str, primary_source_view: str | None = None, *, validate_after: bool = True) -> dict[str, Any]:
    template = get_template(template_id)
    metadata = source_metadata()
    plan = single_view_repair_plan(template, primary_source_view)
    if not plan or not plan.get("primary_source_view"):
        validation = validate_template(template_id, auto_fix=False, live_check=False) if validate_after else None
        return {"status": "skipped", "message": "No multi-view mapping repair was needed.", "template": get_template(template_id), "validation": validation}
    primary_view = plan["primary_source_view"]
    used_pairs = {
        (match["source_view"], match["source_column"])
        for match in selected_matches(template)
        if match.get("source_view") == primary_view
    }
    fields_kept = []
    fields_rematched = []
    fields_unmatched = []
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for match in template.get("matches") or []:
                field_key = str(match.get("field_key") or "")
                if not field_key:
                    continue
                if match.get("source_view") == primary_view and match.get("source_column"):
                    fields_kept.append(
                        {
                            "field_key": field_key,
                            "field_name": match.get("field_name"),
                            "source_column": match.get("source_column"),
                            "confidence": match.get("confidence"),
                        }
                    )
                    continue
                if match.get("source_view") and match.get("source_column"):
                    replacement = candidate_for_view(
                        str(match.get("field_name") or ""),
                        primary_view,
                        metadata.get("views") or [],
                        candidates=match.get("candidates") or [],
                        used_pairs=used_pairs,
                    )
                    if replacement:
                        used_pairs.add((replacement["source_view"], replacement["source_column"]))
                        _update_match_from_candidate(
                            cursor,
                            template_id,
                            field_key,
                            replacement,
                            f"{REPORT_BUILDER_AUTO_FIX_MESSAGE} Rematched from {match.get('source_view')}.{match.get('source_column')} to {primary_view}.{replacement.get('source_column')}.",
                        )
                        fields_rematched.append(
                            {
                                "field_key": field_key,
                                "field_name": match.get("field_name"),
                                "from_source_view": match.get("source_view"),
                                "from_source_column": match.get("source_column"),
                                "to_source_view": primary_view,
                                "to_source_column": replacement.get("source_column"),
                                "confidence": replacement.get("confidence"),
                            }
                        )
                    else:
                        _mark_match_optional_unmatched(
                            cursor,
                            template_id,
                            field_key,
                            f"{REPORT_BUILDER_AUTO_FIX_MESSAGE} No matching column exists in {primary_view}; field marked optional/unmatched.",
                        )
                        fields_unmatched.append(
                            {
                                "field_key": field_key,
                                "field_name": match.get("field_name"),
                                "from_source_view": match.get("source_view"),
                                "from_source_column": match.get("source_column"),
                            }
                        )
            cursor.execute(
                "UPDATE report_templates SET status = 'mapping_in_progress', updated_at = now() WHERE id = %s",
                (template_id,),
            )
    fix = {
        **plan,
        "message": REPORT_BUILDER_AUTO_FIX_MESSAGE,
        "fields_kept": fields_kept,
        "fields_rematched": fields_rematched,
        "fields_unmatched": fields_unmatched,
    }
    duplicate_fix = auto_fix_duplicate_mappings(template_id, metadata)
    if duplicate_fix:
        fix["duplicate_fix"] = duplicate_fix
    validation = validate_template(template_id, auto_fix=False, live_check=False) if validate_after else None
    repaired = get_template(template_id)
    audit_event(template_id, "single_view_mapping_fix_applied", REPORT_BUILDER_AUTO_FIX_MESSAGE, fix)
    return {
        "status": "ok",
        "message": REPORT_BUILDER_AUTO_FIX_MESSAGE,
        "template": repaired,
        "validation": validation,
        "fix": fix,
    }


def auto_fix_template_validation(
    template_id: str,
    primary_source_view: str | None = None,
    *,
    metadata: dict[str, Any] | None = None,
    time_budget_ms: int = 2500,
) -> dict[str, Any]:
    started = time.monotonic()
    metadata = metadata or source_metadata()
    template = get_template(template_id)
    fixes = []
    mapped_source_views = sorted({match["source_view"] for match in selected_matches(template) if match.get("source_view")})
    if len(mapped_source_views) > 1:
        single_view_fix = apply_single_view_fix(template_id, primary_source_view, validate_after=False)
        if single_view_fix.get("fix"):
            fixes.append(single_view_fix["fix"])
        template = get_template(template_id)
    invalid_fix = auto_fix_invalid_mapped_columns(
        template_id,
        metadata=metadata,
        time_budget_ms=time_budget_ms,
    )
    if invalid_fix:
        fixes.append(invalid_fix)
        template = get_template(template_id)
    missing_fix = auto_fix_missing_fields(
        template_id,
        metadata=metadata,
        primary_source_view=primary_source_view,
        time_budget_ms=time_budget_ms,
    )
    if missing_fix:
        fixes.append(missing_fix)
    validation = validate_template(template_id, auto_fix=False, auto_fix_details=fixes, live_check=False)
    validation["timings_ms"] = {
        **(validation.get("timings_ms") or {}),
        "auto_fix": timing_ms(started),
    }
    return {
        "status": "ok" if validation.get("ready") else "needs_review",
        "message": "Missing fields auto-fixed where possible." if fixes else "No automatic validation fix was needed.",
        "template": get_template(template_id),
        "validation": validation,
        "auto_fix": {"applied": bool(fixes), "fixes": fixes},
    }


def validate_template(
    template_id: str,
    *,
    auto_fix: bool = True,
    auto_fix_details: list[dict[str, Any]] | None = None,
    live_check: bool = True,
    allow_unresolved: bool = False,
    mode: str = "report_validation",
) -> dict[str, Any]:
    started = time.monotonic()
    timings: dict[str, int] = {}
    if auto_fix:
        auto_fix_started = time.monotonic()
        template_before = get_template(template_id)
        mapped_source_views = sorted({match["source_view"] for match in selected_matches(template_before) if match.get("source_view")})
        quick_metadata = lightweight_metadata_for_template_sources(template_before)
        schema_errors, _ = validate_mapped_columns_against_schema(template_before, quick_metadata, live_check=True)
        has_missing_required = any(
            match.get("required")
            and not match.get("intentionally_unmapped")
            and not (match.get("source_view") and match.get("source_column"))
            for match in template_before.get("matches") or []
        )
        if has_missing_required or schema_errors or len(mapped_source_views) > 1:
            auto_result = auto_fix_template_validation(template_id, metadata=source_metadata())
            validation = auto_result["validation"]
            validation["template"] = auto_result["template"]
            validation["auto_fix"] = auto_result["auto_fix"]
            validation["auto_fix_applied"] = bool(auto_result["auto_fix"].get("applied"))
            validation["auto_fix_message"] = auto_result["message"]
            validation["timings_ms"] = {
                **(validation.get("timings_ms") or {}),
                "auto_fix": timing_ms(auto_fix_started),
            }
            return validation
        timings["auto_fix"] = timing_ms(auto_fix_started)

    read_started = time.monotonic()
    template = get_template(template_id)
    timings["read_mappings"] = timing_ms(read_started)
    validate_started = time.monotonic()
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    matches = template.get("matches") or []
    mapped = selected_matches(template)
    outputs = output_fields(template)
    unmatched_required = [
        match
        for match in matches
        if match.get("required") and not match.get("intentionally_unmapped") and not (match.get("source_view") and match.get("source_column"))
    ]

    for match in matches:
        if match in unmatched_required:
            unresolved_item = {
                "field": match["field_name"],
                "type": "missing_required_field",
                "message": f"Missing field: {match['field_name']}",
            }
            if allow_unresolved:
                warnings.append({**unresolved_item, "message": f"Unresolved field kept out of generated form: {match['field_name']}"})
            else:
                errors.append(unresolved_item)
        if match.get("sensitive_warning"):
            warnings.append({"field": match["field_name"], "type": "sensitive_field", "message": "Sensitive / confirm allowed"})
        if match.get("source_view") and match.get("source_column") and float(match.get("confidence") or 0) < 0.60:
            warnings.append(
                {
                    "field": match["field_name"],
                    "type": "weak_match",
                    "message": f"Weak match selected: {match.get('source_view')}.{match.get('source_column')} ({round(float(match.get('confidence') or 0) * 100)}%).",
                }
            )

    seen: dict[tuple[str, str], str] = {}
    for match in mapped:
        key = (match["source_view"], match["source_column"])
        if key in seen:
            warnings.append(
                {
                    "field": match["field_name"],
                    "type": "duplicate_mapping",
                    "message": f"Also mapped by {seen[key]}; kept as a warning because output columns remain unique.",
                }
            )
        seen[key] = match["field_name"]

    source_views = sorted({match["source_view"] for match in mapped})
    if not outputs:
        errors.append(
            {
                "type": "missing_output_fields",
                "message": "Map at least one valid field before preview or generation.",
            }
        )
    if not source_views:
        errors.append(
            {
                "type": "missing_source_dataset",
                "message": "No safe view is selected for mapped fields.",
            }
        )
    if len(source_views) > 1:
        repair_plan = single_view_repair_plan(template)
        field_groups = source_view_field_groups(mapped)
        errors.append(
            {
                "type": "cross_view_mapping_not_supported",
                "message": "Report Builder v1 generates a single-source SQL view. Cross-view mappings must be auto-fixed to one safe view before generation.",
                "source_views": source_views,
                "field_groups": field_groups,
                "suggested_fix": repair_plan,
            }
        )
    metadata = source_metadata() if (not live_check or len(source_views) > 1) else lightweight_metadata_for_template_sources(template)

    label_counts: dict[str, list[str]] = {}
    for match in matches:
        label_counts.setdefault(str(match.get("field_name") or "").strip().lower(), []).append(match.get("field_name"))
    duplicate_labels = sorted({labels[0] for labels in label_counts.values() if len(labels) > 1 and labels[0]})
    for label in duplicate_labels:
        errors.append(
            {
                "field": label,
                "type": "duplicate_report_label",
                "message": "Generated SQL views require unique output column labels in v1. Rename duplicate requested fields before preview or generation.",
            }
        )

    timings["validate_fields"] = timing_ms(validate_started)
    field_discovery_started = time.monotonic()
    checked_views: dict[str, dict[str, Any]] = {}
    schema_errors, checked_views = validate_mapped_columns_against_schema(template, metadata, live_check=live_check)
    errors.extend(schema_errors)
    if live_check:
        for source_view in source_views:
            try:
                checked = checked_views.get(source_view) or {}
                column_details = checked.get("details") or safe_view_column_details(source_view, [])
                column_detail_by_name = {str(item.get("name") or ""): item for item in column_details}
                columns = set(column_detail_by_name)
                trino_query(f"SELECT 1 FROM delta.silver.{quote_identifier(source_view)} LIMIT 1")
                for match in [item for item in mapped if item["source_view"] == source_view]:
                    if match["source_column"] not in columns:
                        continue
                    detail = column_detail_by_name.get(match["source_column"]) or {}
                    expected_type = expected_data_type_group(str(match.get("field_name") or ""))
                    actual_type = actual_data_type_group(str(detail.get("data_type") or "unknown"))
                    if expected_type and actual_type:
                        _, type_reason = data_type_compatibility(expected_type, actual_type)
                        if type_reason == "datatype may not match":
                            warnings.append(
                                {
                                    "field": match["field_name"],
                                    "type": "datatype_mismatch",
                                    "message": f"Data type may not match: expected {expected_type}, found {actual_type} on {source_view}.{match['source_column']}.",
                                }
                            )
            except Exception as exc:
                errors.append({"type": "broken_source_view", "source_view": source_view, "message": f"Invalid table: {source_view}. {exc}"})
    else:
        warnings.append(
            {
                "type": "validation_live_check_deferred",
                "message": "Auto Fix completed without a blocking live Trino recheck; Preview will verify the generated SQL.",
            }
        )
    timings["field_discovery"] = timing_ms(field_discovery_started)
    for source_view in source_views:
        if is_child_view(source_view):
            warnings.append(
                {
                    "type": "one_to_many_expansion_risk",
                    "source_view": source_view,
                    "message": "This mapping uses an expanded child table and may multiply parent rows.",
                }
            )

    generated_sql = None
    sql_validation = None
    schema_started = time.monotonic()
    if not errors and len(source_views) == 1 and outputs:
        try:
            generated_sql = preview_sql_for_view(source_views[0], outputs, 20)
            sql_validation = validate_generated_select_sql(source_views[0], outputs)
            if sql_validation.get("status") != "ok":
                errors.append(
                    {
                        "type": "generated_sql_invalid",
                        "message": sql_validation.get("message") or "Generated SQL validation failed.",
                        "source_view": source_views[0],
                        "generated_sql": sql_validation.get("generated_sql"),
                        "error_message": sql_validation.get("error_message"),
                    }
                )
        except Exception as exc:
            errors.append({"type": "sql_generation_failed", "message": str(exc)})
    timings["schema_generation"] = timing_ms(schema_started)

    status = "failed" if errors else "ready"
    table_errors = [error for error in errors if error.get("type") in {"missing_source_dataset", "invalid_trino_reference", "broken_source_view", "missing_source_view", "missing_mapped_column", "cross_view_mapping_not_supported", "generated_sql_invalid"}]
    datatype_warnings = [warning for warning in warnings if warning.get("type") == "datatype_mismatch"]
    relationship_warnings = [warning for warning in warnings if warning.get("type") in {"incompatible_join_risk", "duplicate_mapping", "one_to_many_expansion_risk"}]
    validation_steps = [
        validation_step(
            "Field mappings verified",
            not unmatched_required and bool(mapped),
            "Field mappings verified" if not unmatched_required and mapped else "Missing required field mappings.",
            detail=", ".join(str(item.get("field_name") or "") for item in unmatched_required) or None,
        ),
        validation_step(
            "Tables verified",
            not table_errors and bool(source_views),
            "Tables verified" if not table_errors and source_views else "One or more selected tables/views are invalid or missing.",
        ),
        validation_step(
            "Data types verified",
            not datatype_warnings,
            "Data types verified" if not datatype_warnings else "Some selected columns may have incompatible data types.",
        ),
        validation_step(
            "Relationships verified",
            not relationship_warnings,
            "Relationships verified" if not relationship_warnings else "Review duplicate, expanded child, or cross-view relationships.",
        ),
    ]
    result = {
        "status": status,
        "ready": not errors,
        "errors": errors,
        "warnings": warnings,
        "validation_steps": validation_steps,
        "summary_message": "Validation Completed Successfully" if not errors else "Validation Failed",
        "source_views": source_views,
        "field_groups": source_view_field_groups(mapped),
        "suggested_fix": single_view_repair_plan(template) if len(source_views) > 1 else None,
        "selected_safe_view": source_views[0] if len(source_views) == 1 else None,
        "mapped_fields": [
            {
                "field_key": match.get("field_key"),
                "field_name": match.get("field_name"),
                "source_view": match.get("source_view"),
                "source_column": match.get("source_column"),
                "confidence": match.get("confidence"),
            }
            for match in mapped
        ],
        "unmatched_fields": unmatched_fields(template),
        "mapped_field_count": len(mapped),
        "unmatched_field_count": len(unmatched_fields(template)),
        "generated_sql": generated_sql,
        "sql_validation": sql_validation,
        "schema_validation": {
            "checked_views": [
                {
                    "source_view": source_view,
                    "column_count": len((details or {}).get("columns") or []),
                    "source": (details or {}).get("source"),
                    "metadata_known": bool((details or {}).get("metadata_known")),
                }
                for source_view, details in checked_views.items()
            ],
            "errors": schema_errors,
        },
        "auto_fix": {"applied": bool(auto_fix_details), "fixes": auto_fix_details or []},
        "auto_fix_applied": bool(auto_fix_details),
        "auto_fix_message": REPORT_BUILDER_AUTO_FIX_MESSAGE if auto_fix_details else None,
        "mode": mode,
        "allow_unresolved": allow_unresolved,
        "timings_ms": timings,
        "duration_ms": timing_ms(started),
    }
    if auto_fix_details:
        result["template"] = template
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE report_templates
                SET status = %s, validation_json = %s, updated_at = now()
                WHERE id = %s
                """,
                ("ready" if status == "ready" else "needs_review", Json(json_safe(result)), template_id),
            )
    audit_event(template_id, "validation_run", f"Template validation {status}", {"errors": len(errors), "warnings": len(warnings), "auto_fix_applied": bool(auto_fix_details), "mode": mode, "timings_ms": timings, "duration_ms": timing_ms(started)})
    print(safe_json_dumps({"event": "report_template_validation_timing", "template_id": template_id, "mode": mode, "timings_ms": timings, "duration_ms": timing_ms(started)}, sort_keys=True))
    return result


def report_select_sql(outputs: list[dict[str, Any]]) -> str:
    return ", ".join(
        f"{quote_identifier(match['source_column'])} AS {quote_identifier(match['field_name'])}"
        if match.get("output_kind") == "mapped"
        else f"CAST(NULL AS varchar) AS {quote_identifier(match['field_name'])}"
        for match in outputs
    )


def report_select_sql_lines(outputs: list[dict[str, Any]]) -> str:
    return ",\n    ".join(
        f"{quote_identifier(match['source_column'])} AS {quote_identifier(match['field_name'])}"
        if match.get("output_kind") == "mapped"
        else f"CAST(NULL AS varchar) AS {quote_identifier(match['field_name'])}"
        for match in outputs
    )


def select_sql_for_view(source_view: str, outputs: list[dict[str, Any]], limit: int | None = None) -> str:
    sql = f"SELECT {report_select_sql(outputs)} FROM delta.silver.{quote_identifier(source_view)}"
    if limit is not None:
        sql = f"{sql} LIMIT {max(int(limit), 0)}"
    return sql


def preview_sql_for_view(source_view: str, outputs: list[dict[str, Any]], limit: int) -> str:
    capped_limit = min(max(int(limit), 1), 20)
    return select_sql_for_view(source_view, outputs, capped_limit)


def validate_generated_select_sql(source_view: str, outputs: list[dict[str, Any]]) -> dict[str, Any]:
    sql = select_sql_for_view(source_view, outputs, 0)
    trace = [
        {
            "requested_field": match.get("field_name"),
            "mapped_table": match.get("source_view"),
            "mapped_column": match.get("source_column"),
            "sql_fragment": sql_fragment_for_mapping(match),
        }
        for match in outputs
    ]
    try:
        trino_query(sql)
        return {
            "status": "ok",
            "message": "SQL valid",
            "source_view": source_view,
            "generated_sql": sql,
            "field_trace": trace,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "message": "Generated SQL failed Trino validation.",
            "source_view": source_view,
            "generated_sql": sql,
            "error_message": str(exc),
            "field_trace": trace,
        }


def trace_report_generation_sql(template_id: str, source_view: str, outputs: list[dict[str, Any]], generated_sql: str) -> list[dict[str, Any]]:
    trace = [
        {
            "template_id": template_id,
            "requested_field": match.get("field_name"),
            "field_key": match.get("field_key"),
            "mapped_table": match.get("source_view"),
            "mapped_column": match.get("source_column"),
            "confidence": match.get("confidence"),
            "match_type": match.get("match_type"),
            "match_reason": match.get("match_reason"),
            "generated_source_view": source_view,
            "sql_fragment": sql_fragment_for_mapping(match),
        }
        for match in outputs
    ]
    print(
        safe_json_dumps(
            {
                "event": "report_generation_sql_trace",
                "template_id": template_id,
                "source_view": source_view,
                "generated_sql": generated_sql,
                "fields": trace,
            },
            sort_keys=True,
        )
    )
    return trace


def generated_view_preview(view_name: str, limit: int = 20) -> dict[str, Any]:
    sql = f"SELECT * FROM delta.silver.{quote_identifier(view_name)} LIMIT {min(max(int(limit), 1), 20)}"
    columns, rows, _ = trino_query(sql, observe=True, user_source="report_generation_preview", selected_view=view_name)
    shaped_rows = [dict(zip(columns, row)) for row in rows]
    return {"status": "ok", "columns": columns, "rows": shaped_rows, "generated_sql": sql, "row_count": len(shaped_rows)}


def preview_template(template_id: str, limit: int = 20) -> dict[str, Any]:
    validation = validate_template(template_id)
    if not validation["ready"]:
        return {"status": "failed", "validation": validation, "columns": [], "rows": [], "generated_sql": None}
    template = get_template(template_id)
    mapped = selected_matches(template)
    outputs = output_fields(template)
    unmatched = unmatched_fields(template)
    source_view = mapped[0]["source_view"]
    sql = preview_sql_for_view(source_view, outputs, limit)
    try:
        columns, rows, _ = trino_query(sql, observe=True, user_source="report_template_preview", selected_view=source_view)
        shaped_rows = [dict(zip(columns, row)) for row in rows]
        null_counts = {column: sum(1 for row in shaped_rows if row.get(column) is None) for column in columns}
        audit_event(template_id, "preview_run", "Report preview run through Trino", {"row_count": len(shaped_rows), "source_view": source_view})
        return {
            "status": "ok",
            "message": f"Preview generated with {len(mapped)} mapped fields, {len(unmatched)} unmatched fields.",
            "validation": validation,
            "source_view": source_view,
            "columns": columns,
            "rows": shaped_rows,
            "null_counts": null_counts,
            "generated_sql": sql,
            "mapped_field_count": len(mapped),
            "unmatched_field_count": len(unmatched),
            "unmatched_fields": unmatched,
        }
    except Exception as exc:
        audit_event(template_id, "preview_failed", "Report preview failed in Trino", {"error_message": str(exc), "source_view": source_view})
        return {
            "status": "failed",
            "validation": validation,
            "source_view": source_view,
            "columns": [],
            "rows": [],
            "generated_sql": sql,
            "error_message": f"Trino preview query failed: {exc}",
            "mapped_field_count": len(mapped),
            "unmatched_field_count": len(unmatched),
            "unmatched_fields": unmatched,
        }


def generated_view_name(template_name: str, template_id: str) -> str:
    return safe_identifier("report", template_name, str(template_id).replace("-", "")[:12], "analytics", max_length=110)


def publish_report_to_superset(
    *,
    template_id: str,
    run_id: str,
    view_name: str,
    row_count: int,
    columns_count: int,
    timings: dict[str, int],
) -> None:
    update_generation_run_progress(
        run_id,
        step="dataset_registration",
        progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["dataset_registration"],
        current_action="Registering Superset dataset",
        superset_status="running",
        timings=timings,
    )
    audit_event(template_id, "superset_publication_attempted", "Superset dataset publication attempted", {"generated_view_name": view_name, "run_id": run_id})
    try:
        stage_started = time.monotonic()
        client = SupersetClient()
        database_id = ensure_superset_database(client)
        superset_dataset_id = ensure_superset_dataset(client, database_id, view_name)
        report_superset_url = superset_url(f"/explore/?dataset_type=table&dataset_id={superset_dataset_id}")
        timings["dataset_registration"] = timing_ms(stage_started)
        update_generation_run_progress(
            run_id,
            step="superset_api_calls",
            progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["superset_api_calls"],
            current_action="Creating Superset charts and dashboard",
            timings=timings,
            superset_status="dataset_generated",
            superset_dataset_id=superset_dataset_id,
        )
        upsert_bi_dataset(view_name, superset_dataset_id=superset_dataset_id, row_count=row_count, columns_count=columns_count, status="ok")
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE report_templates
                    SET superset_dataset_id = %s, superset_url = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (superset_dataset_id, report_superset_url, template_id),
                )
                cursor.execute(
                    """
                    UPDATE report_generations
                    SET superset_dataset_id = %s, superset_url = %s
                    WHERE template_id = %s AND generated_view_name = %s
                    """,
                    (superset_dataset_id, report_superset_url, template_id, view_name),
                )
        audit_event(template_id, "superset_publication_succeeded", "Superset dataset publication succeeded", {"superset_dataset_id": superset_dataset_id, "run_id": run_id})

        stage_started = time.monotonic()
        superset_status = "dataset_generated"
        try:
            dashboard_spec = dynamic_dashboard_spec_for_dataset(view_name)
            if dashboard_spec:
                dashboards = ensure_superset_dashboards(client, {view_name: superset_dataset_id}, dashboard_specs=[dashboard_spec], new_only=False)
                dashboard_result = dashboards[0] if dashboards else None
                superset_status = "dashboard_generated" if dashboard_result and dashboard_result.get("status") == "ok" else "dashboard_failed"
            audit_event(template_id, "superset_assets_generated", "Superset starter assets generated", {"status": superset_status, "run_id": run_id})
        except Exception as dashboard_exc:
            superset_status = "dashboard_failed"
            audit_event(template_id, "superset_dashboard_generation_failed", "Superset dashboard generation failed", {"error_message": str(dashboard_exc), "run_id": run_id})
            update_generation_run_progress(run_id, step="superset_api_calls", superset_status=superset_status, superset_error_message=str(dashboard_exc), timings=timings)
        timings["superset_api_calls"] = timing_ms(stage_started)

        stage_started = time.monotonic()
        update_generation_run_progress(
            run_id,
            step="metadata_updates",
            progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["metadata_updates"],
            current_action="Superset publication complete",
            timings=timings,
            superset_status=superset_status,
            superset_dataset_id=superset_dataset_id,
        )
        timings["metadata_updates"] = timings.get("metadata_updates", 0) + timing_ms(stage_started)
        update_generation_run_progress(
            run_id,
            step="done",
            progress_percent=100,
            current_action="Report generation complete",
            timings=timings,
            superset_status=superset_status,
            superset_dataset_id=superset_dataset_id,
        )
        log_generation_timing(run_id, template_id, timings)
    except Exception as exc:
        audit_event(template_id, "superset_publication_failed", "Superset dataset publication failed", {"error_message": str(exc), "run_id": run_id})
        upsert_bi_dataset(view_name, row_count=row_count, columns_count=columns_count, status="ok", error_message=f"Superset publication failed: {exc}")
        update_generation_run_progress(
            run_id,
            step="superset_failed",
            progress_percent=100,
            current_action="Report created; Superset publication failed",
            timings=timings,
            superset_status="dataset_failed",
            superset_error_message=str(exc),
        )
        log_generation_timing(run_id, template_id, timings)


def queue_report_superset_publication(
    *,
    template_id: str,
    run_id: str,
    view_name: str,
    row_count: int,
    columns_count: int,
    timings: dict[str, int],
) -> None:
    worker = threading.Thread(
        target=publish_report_to_superset,
        kwargs={
            "template_id": template_id,
            "run_id": run_id,
            "view_name": view_name,
            "row_count": row_count,
            "columns_count": columns_count,
            "timings": dict(timings),
        },
        daemon=True,
    )
    worker.start()


def generate_report(template_id: str, publish_to_superset: bool = True, confirm_regenerate: bool = False) -> dict[str, Any]:
    init_report_template_db()
    started = time.monotonic()
    timings: dict[str, int] = {}
    template = get_template(template_id)
    if template.get("generated_view_name") and template.get("generation_status") in {"generated", "needs_regeneration"} and not confirm_regenerate:
        audit_event(template_id, "generation_blocked", "Regeneration requires explicit confirmation", {"generated_view_name": template.get("generated_view_name")})
        return {
            "status": "needs_confirmation",
            "message": "This template already has generated report assets. Confirm regeneration before overwriting them.",
            "generated_view_name": template.get("generated_view_name"),
            "validation": template.get("validation_json") or {},
        }
    run_id = new_id()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO report_generation_runs (
                    id, template_id, status, progress_percent, progress_step, current_action, timings_json, superset_status
                )
                VALUES (%s, %s, 'running', 1, 'mapping_validation', 'Validating mappings', '{}'::jsonb, %s)
                """,
                (run_id, template_id, "queued" if publish_to_superset else "skipped"),
            )

    stage_started = time.monotonic()
    validation = validate_template(template_id)
    timings["mapping_validation"] = timing_ms(stage_started)
    update_generation_run_progress(
        run_id,
        step="schema_discovery",
        progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["schema_discovery"],
        current_action="Reading cached schema and source metadata",
        timings=timings,
    )
    template = get_template(template_id)
    view_name = generated_view_name(template["template_name"], template_id)
    if not validation["ready"]:
        timings["metadata_updates"] = timing_ms(started) - sum(timings.values())
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE report_generation_runs
                    SET status = 'failed', finished_at = now(), duration_ms = %s,
                        validation_json = %s, error_message = %s, progress_percent = 100,
                        progress_step = 'failed', current_action = 'Validation failed',
                        timings_json = %s, progress_updated_at = now()
                    WHERE id = %s
                    """,
                    (timing_ms(started), Json(json_safe(validation)), "Validation failed", Json(json_safe(timings)), run_id),
                )
        log_generation_timing(run_id, template_id, timings)
        return {"status": "failed", "validation": validation, "generated_view_name": None}

    try:
        stage_started = time.monotonic()
        # Validation already checked the selected source schema; generation only needs
        # the saved mappings from this template, not a full safe-view metadata reload.
        timings["schema_discovery"] = timing_ms(stage_started)
        update_generation_run_progress(
            run_id,
            step="smart_match_processing",
            progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["smart_match_processing"],
            current_action="Reusing saved Smart Match mappings",
            timings=timings,
        )
        stage_started = time.monotonic()
        mapped = selected_matches(template)
        outputs = output_fields(template)
        unmatched = unmatched_fields(template)
        source_view = mapped[0]["source_view"]
        timings["smart_match_processing"] = timing_ms(stage_started)

        update_generation_run_progress(
            run_id,
            step="sql_generation",
            progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["sql_generation"],
            current_action="Creating generated SQL view",
            timings=timings,
        )
        stage_started = time.monotonic()
        select_sql = report_select_sql_lines(outputs)
        sql = f"CREATE OR REPLACE VIEW delta.silver.{quote_identifier(view_name)} AS\nSELECT\n    {select_sql}\nFROM delta.silver.{quote_identifier(source_view)}"
        sql_trace = trace_report_generation_sql(template_id, source_view, outputs, sql)
        sql_validation = validate_generated_select_sql(source_view, outputs)
        if sql_validation.get("status") != "ok":
            timings["sql_generation"] = timing_ms(stage_started)
            validation = {
                **validation,
                "ready": False,
                "status": "failed",
                "summary_message": "Generated SQL validation failed",
                "sql_validation": sql_validation,
                "errors": [
                    *(validation.get("errors") or []),
                    {
                        "type": "generated_sql_invalid",
                        "message": sql_validation.get("message") or "Generated SQL validation failed.",
                        "source_view": source_view,
                        "generated_sql": sql_validation.get("generated_sql"),
                        "error_message": sql_validation.get("error_message"),
                        "field_trace": sql_trace,
                    },
                ],
            }
            timings["metadata_updates"] = max(0, timing_ms(started) - sum(timings.values()))
            with dashboard_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE report_generation_runs
                        SET status = 'failed', finished_at = now(), duration_ms = %s,
                            validation_json = %s, error_message = %s, progress_percent = 100,
                            progress_step = 'failed', current_action = 'Generated SQL validation failed',
                            timings_json = %s, progress_updated_at = now()
                        WHERE id = %s
                        """,
                        (timing_ms(started), Json(json_safe(validation)), sql_validation.get("error_message") or "Generated SQL validation failed", Json(json_safe(timings)), run_id),
                    )
                    cursor.execute(
                        "UPDATE report_templates SET status = 'needs_review', generation_status = 'failed', validation_json = %s, updated_at = now() WHERE id = %s",
                        (Json(json_safe(validation)), template_id),
                    )
            audit_event(template_id, "report_generation_sql_validation_failed", "Generated SQL validation failed", {"generated_view_name": view_name, "run_id": run_id, "sql_validation": sql_validation, "field_trace": sql_trace, "timings_ms": timings})
            log_generation_timing(run_id, template_id, timings)
            return {"status": "failed", "run_id": run_id, "generated_view_name": None, "validation": validation, "error_message": sql_validation.get("error_message"), "timings_ms": timings}
        try:
            trino_query(sql)
        except Exception:
            trino_query(f"DROP VIEW IF EXISTS delta.silver.{quote_identifier(view_name)}")
            trino_query(sql.replace("CREATE OR REPLACE VIEW", "CREATE VIEW", 1))
        timings["sql_generation"] = timing_ms(stage_started)

        superset_dataset_id = None
        report_superset_url = None
        superset_result: dict[str, Any] = {
            "status": "queued" if publish_to_superset else "skipped",
            "message": "Superset publication is running in the background." if publish_to_superset else "Superset publication was not requested.",
            "dataset_id": None,
            "dataset_url": None,
            "dashboard": None,
            "background": bool(publish_to_superset),
            "run_id": run_id,
        }

        update_generation_run_progress(
            run_id,
            step="report_save",
            progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["report_save"],
            current_action="Saving report definition",
            timings=timings,
        )
        stage_started = time.monotonic()
        row_count = source_view_row_count_from_cache(source_view)
        columns_count = len(outputs)
        upsert_bi_dataset(view_name, row_count=row_count, columns_count=columns_count, status="pending" if publish_to_superset else "ok")
        if publish_to_superset:
            update_generation_run_progress(
                run_id,
                step="dataset_registration",
                progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["dataset_registration"],
                current_action="Superset publication queued",
                timings=timings,
                superset_status="queued",
            )

        try:
            preview_result = generated_view_preview(view_name, 20)
        except Exception as preview_exc:
            preview_result = {
                "status": "failed",
                "columns": [],
                "rows": [],
                "generated_sql": f"SELECT * FROM delta.silver.{quote_identifier(view_name)} LIMIT 20",
                "error_message": f"Generated view exists, but preview failed: {preview_exc}",
            }
        timings["report_save"] = timing_ms(stage_started)

        duration_ms = int((time.monotonic() - started) * 1000)
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO report_generations (
                        id, template_id, generated_view_name, superset_dataset_id, superset_url, status
                    )
                    VALUES (%s, %s, %s, %s, %s, 'generated')
                    """,
                    (new_id(), template_id, view_name, superset_dataset_id, report_superset_url),
                )
                cursor.execute(
                    """
                    UPDATE report_templates
                    SET status = 'generated', generation_status = 'generated', generated_view_name = %s,
                        superset_dataset_id = %s, superset_url = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (view_name, superset_dataset_id, report_superset_url, template_id),
                )
                cursor.execute(
                    """
                    UPDATE report_generation_runs
                    SET status = 'generated', finished_at = now(), duration_ms = %s,
                        validation_json = %s, generated_view_name = %s, superset_dataset_id = %s,
                        progress_percent = %s, progress_step = %s, current_action = %s,
                        timings_json = %s, superset_status = %s, progress_updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        duration_ms,
                        Json(json_safe(validation)),
                        view_name,
                        superset_dataset_id,
                        REPORT_GENERATION_PROGRESS_WEIGHTS["dataset_registration"] if publish_to_superset else 100,
                        "dataset_registration" if publish_to_superset else "done",
                        "Report created; Superset publication queued" if publish_to_superset else "Report generation complete",
                        Json(json_safe(timings)),
                        "queued" if publish_to_superset else "skipped",
                        run_id,
                    ),
                )
        timings["metadata_updates"] = max(0, timing_ms(started) - sum(timings.values()))
        update_generation_run_progress(
            run_id,
            step="dataset_registration" if publish_to_superset else "done",
            progress_percent=REPORT_GENERATION_PROGRESS_WEIGHTS["dataset_registration"] if publish_to_superset else 100,
            current_action="Report created; Superset publication queued" if publish_to_superset else "Report generation complete",
            timings=timings,
            superset_status="queued" if publish_to_superset else "skipped",
        )
        audit_event(template_id, "report_generated", "Report view generated", {"generated_view_name": view_name, "row_count": row_count, "columns_count": columns_count, "run_id": run_id, "timings_ms": timings})
        log_generation_timing(run_id, template_id, timings)
        if publish_to_superset:
            queue_report_superset_publication(
                template_id=template_id,
                run_id=run_id,
                view_name=view_name,
                row_count=row_count,
                columns_count=columns_count,
                timings=timings,
            )
        return {
            "status": "generated",
            "message": "Report Created Successfully",
            "run_id": run_id,
            "generated_view_name": view_name,
            "source_view": source_view,
            "selected_safe_view": source_view,
            "mapped_fields": validation.get("mapped_fields") or mapped,
            "unmatched_fields": unmatched,
            "warnings": validation.get("warnings") or [],
            "row_count": row_count,
            "columns_count": columns_count,
            "superset_dataset_id": superset_dataset_id,
            "superset_url": report_superset_url,
            "superset": superset_result,
            "preview": preview_result,
            "validation": validation,
            "generated_sql": sql,
            "timings_ms": timings,
            "progress": get_generation_run(run_id),
        }
    except Exception as exc:
        timings["metadata_updates"] = max(0, timing_ms(started) - sum(timings.values()))
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE report_generation_runs
                    SET status = 'failed', finished_at = now(), duration_ms = %s, error_message = %s,
                        validation_json = %s, progress_percent = 100, progress_step = 'failed',
                        current_action = 'Report generation failed', timings_json = %s,
                        progress_updated_at = now()
                    WHERE id = %s
                    """,
                    (timing_ms(started), str(exc), Json(json_safe(validation)), Json(json_safe(timings)), run_id),
                )
                cursor.execute(
                    "UPDATE report_templates SET status = 'failed', generation_status = 'failed', updated_at = now() WHERE id = %s",
                    (template_id,),
                )
        audit_event(template_id, "report_generation_failed", "Report generation failed", {"error_message": str(exc), "generated_view_name": view_name, "run_id": run_id, "timings_ms": timings})
        log_generation_timing(run_id, template_id, timings)
        return {"status": "failed", "run_id": run_id, "generated_view_name": view_name, "validation": validation, "error_message": str(exc), "timings_ms": timings}
