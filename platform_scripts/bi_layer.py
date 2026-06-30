from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from typing import Any

import requests
from psycopg2.extras import DictCursor, Json

from common import find_non_json_safe_fields, json_safe, load_environment, safe_json_dumps, setup_logging, trino_connection
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db, new_id
from query_layer import (
    is_pii_column,
    quote_identifier,
    refresh_query_layer,
    safe_views_detail,
    table_columns,
    table_row_count,
    trino_query,
)


load_environment()


SUPERSET_CONTAINER_NAME = os.environ.get("SUPERSET_CONTAINER_NAME", "local-data-platform-superset")
SUPERSET_INTERNAL_URL = os.environ.get("SUPERSET_INTERNAL_URL", "http://superset:8088").rstrip("/")
SUPERSET_EXTERNAL_URL = os.environ.get("SUPERSET_EXTERNAL_URL", f"http://localhost:{os.environ.get('SUPERSET_PORT', '8088')}").rstrip("/")
SUPERSET_USERNAME = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")
SUPERSET_TRINO_USER = os.environ.get("SUPERSET_TRINO_USER", "superset")
SUPERSET_TRINO_URI = os.environ.get("SUPERSET_TRINO_SQLALCHEMY_URI", f"trino://{SUPERSET_TRINO_USER}@trino:8080/delta/silver")
SUPERSET_DATABASE_NAME = os.environ.get("SUPERSET_DATABASE_NAME", "ONOV8 Trino Safe Analytics")
SUPERSET_SCHEMA = os.environ.get("SUPERSET_SCHEMA", "silver")
DASHBOARD_LOAD_THRESHOLD_MS = int(os.environ.get("BI_DASHBOARD_LOAD_THRESHOLD_MS", "8000"))
SUPERSET_DASHBOARD_BACKGROUND = "BACKGROUND_TRANSPARENT"
BI_GENERATED_BY = "dynamic_starter"
BI_GENERATED_MARKER = "ONOV8_DYNAMIC_STARTER_BI"
BI_SUPPORTED_VIZ_TYPES = {"big_number_total", "echarts_pie", "echarts_timeseries_bar", "echarts_timeseries_line", "table"}
BI_MAX_CHARTS_PER_DATASET = int(os.environ.get("BI_MAX_CHARTS_PER_DATASET", "4"))
DASHBOARD_GENERATION_DISABLED_MESSAGE = "Automatic dashboard generation is disabled; dashboard recommendations will be generated later from selected datasets."
EXPANDED_CHILD_DATASET_MARKERS = ("stakeholder", "visa", "license", "activity", "activities", "request", "response")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def superset_url(path: str) -> str:
    return f"{SUPERSET_EXTERNAL_URL}{path}"


def metric(label: str, aggregate: str, column_name: str | None = None, sql_expression: str | None = None) -> dict[str, Any]:
    option_name = "metric_" + "".join(character.lower() if character.isalnum() else "_" for character in label).strip("_")
    if sql_expression:
        return {
            "expressionType": "SQL",
            "sqlExpression": sql_expression,
            "label": label,
            "optionName": option_name,
        }
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name} if column_name else None,
        "aggregate": aggregate,
        "label": label,
        "optionName": option_name,
    }


def slugify(value: str, *, max_length: int = 180) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return (slug or "generated")[:max_length].strip("-") or "generated"


def humanize_identifier(value: str) -> str:
    words = [word for word in re.split(r"[_\s]+", value) if word]
    return " ".join(word[:1].upper() + word[1:] for word in words) or value


def dataset_base_name(dataset_name: str) -> str:
    return dataset_name.removesuffix("_analytics")


def dataset_parts(dataset_name: str) -> tuple[str, str]:
    base_name = dataset_base_name(dataset_name)
    if "__" in base_name:
        database, collection = base_name.split("__", 1)
        return database, collection
    return SUPERSET_SCHEMA, base_name


def dashboard_name_for_dataset(dataset_name: str) -> str:
    return f"{dataset_base_name(dataset_name)}_dashboard"


def chart_name_for_dataset(dataset_name: str, suffix: str) -> str:
    name = f"{dataset_base_name(dataset_name)}__{suffix}"
    return name[:240]


def eligible_safe_views(rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    views = rows if rows is not None else safe_views_detail()
    return [
        view
        for view in views
        if view.get("status") == "ok"
        and bool(view.get("pii_safe"))
        and bool(view.get("trino_visible"))
    ]


def safe_view_eligibility_summary(rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    views = rows if rows is not None else safe_views_detail()
    eligible = eligible_safe_views(views)
    non_ok = [view for view in views if view.get("status") != "ok"]
    ok_not_eligible = [
        view
        for view in views
        if view.get("status") == "ok" and view.get("view_name") not in {item["view_name"] for item in eligible}
    ]
    return {
        "total_safe_views": len(views),
        "eligible_safe_views_count": len(eligible),
        "eligible_safe_view_names": sorted(view.get("view_name") for view in eligible if view.get("view_name")),
        "non_ok_safe_views_count": len(non_ok),
        "non_ok_safe_view_names": sorted(view.get("view_name") for view in non_ok if view.get("view_name")),
        "ok_but_not_eligible_count": len(ok_not_eligible),
        "ok_but_not_eligible_names": sorted(view.get("view_name") for view in ok_not_eligible if view.get("view_name")),
    }


def dynamic_safe_datasets() -> tuple[str, ...]:
    return tuple(sorted(view["view_name"] for view in eligible_safe_views()))


def table_column_details(table_name: str) -> list[dict[str, str]]:
    _, rows, _ = trino_query(f"SHOW COLUMNS FROM delta.silver.{quote_identifier(table_name)}")
    return [{"name": str(row[0]), "type": str(row[1]).lower()} for row in rows]


def is_numeric_type(column_type: str) -> bool:
    normalized = column_type.lower()
    return any(token in normalized for token in ("int", "decimal", "double", "real", "number"))


def is_temporal_type(column_type: str) -> bool:
    normalized = column_type.lower()
    return "timestamp" in normalized or normalized == "date"


def is_low_value_text_column(column: dict[str, str]) -> bool:
    name = column["name"].lower()
    column_type = column["type"].lower()
    if name.endswith("_raw_json") or name.endswith("_hash"):
        return False
    if any(marker in name for marker in ("password", "secret", "token", "url", "document", "message", "description", "object_key")):
        return False
    if name == "id" or name.endswith("id") or name.endswith("_id"):
        return False
    enum_markers = (
        "status",
        "state",
        "type",
        "category",
        "country",
        "region",
        "source",
        "domain",
        "provider",
        "portal",
        "gender",
        "theme",
        "role",
        "tier",
        "screen",
        "platform",
        "mode",
        "level",
    )
    return ("varchar" in column_type or "boolean" in column_type) and any(marker in name for marker in enum_markers)


def is_temporal_column(column: dict[str, str]) -> bool:
    name = column["name"].lower()
    if is_temporal_type(column["type"]):
        return True
    if name.endswith("_raw_json") or name.endswith("_hash"):
        return False
    temporal_markers = ("createdat", "updatedat", "timestamp", "datetime", "processed_at", "ingested_at")
    return name.endswith("date") or name.endswith("time") or name.endswith("_at") or any(marker in name for marker in temporal_markers)


def is_measure_column(column: dict[str, str]) -> bool:
    name = column["name"].lower()
    if not is_numeric_type(column["type"]):
        return False
    if name in {"v", "index", "sortorder"} or name.endswith("id") or name.endswith("_id"):
        return False
    measure_markers = (
        "amount",
        "total",
        "count",
        "revenue",
        "commission",
        "score",
        "duration",
        "quantity",
        "price",
        "value",
        "percentage",
        "applications",
        "users",
    )
    return any(marker in name for marker in measure_markers)


def preferred_count_column(columns: list[dict[str, str]]) -> str | None:
    names = [column["name"] for column in columns]
    for preferred in ("silver_row_id", "id", "uuid"):
        if preferred in names:
            return preferred
    for column in columns:
        name = column["name"].lower()
        if not name.endswith("_raw_json") and not name.endswith("_hash"):
            return column["name"]
    return names[0] if names else None


def preferred_temporal_column(columns: list[dict[str, str]]) -> dict[str, str] | None:
    candidates = [column for column in columns if is_temporal_column(column)]
    if not candidates:
        return None
    preferred_order = ("createdat", "created_at", "createddate", "created_date", "timestamp", "updatedat", "updated_at")
    return sorted(candidates, key=lambda column: preferred_order.index(column["name"].lower()) if column["name"].lower() in preferred_order else len(preferred_order))[0]


def preferred_enum_column(columns: list[dict[str, str]]) -> dict[str, str] | None:
    candidates = [column for column in columns if is_low_value_text_column(column)]
    if not candidates:
        return None
    preferred_order = ("status", "type", "domain", "category", "country", "countrycode", "role", "tier", "gender")
    return sorted(candidates, key=lambda column: preferred_order.index(column["name"].lower()) if column["name"].lower() in preferred_order else len(preferred_order))[0]


def preferred_measure_column(columns: list[dict[str, str]]) -> dict[str, str] | None:
    candidates = [column for column in columns if is_measure_column(column)]
    if not candidates:
        return None
    preferred_order = ("amount", "total_amount", "revenue", "revenuegenerated", "totalapplicationscount", "count")
    return sorted(candidates, key=lambda column: preferred_order.index(column["name"].lower()) if column["name"].lower() in preferred_order else len(preferred_order))[0]


def timestamp_sql_expression(column: dict[str, str]) -> str:
    quoted = quote_identifier(column["name"])
    if is_temporal_type(column["type"]):
        return f"CAST({quoted} AS timestamp)"
    return f"COALESCE(try_cast({quoted} AS timestamp), CAST(try(from_iso8601_timestamp({quoted})) AS timestamp))"


def dataset_relation(dataset_name: str) -> str:
    return f"delta.silver.{quote_identifier(dataset_name)}"


def chart_form_data(spec: dict[str, Any], dataset_id: int) -> dict[str, Any]:
    viz_type = spec["viz_type"]
    metrics = spec.get("metrics", [])
    groupby = spec.get("groupby", [])
    form_data: dict[str, Any] = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz_type,
        "metrics": metrics,
        "adhoc_filters": spec.get("filters", []),
        "row_limit": spec.get("row_limit", 1000),
        "time_range": "No filter",
    }
    if viz_type == "big_number_total":
        form_data.update(
            {
                "metric": metrics[0],
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
            }
        )
    elif viz_type == "echarts_pie":
        form_data.update(
            {
                "groupby": groupby,
                "metric": metrics[0],
                "show_legend": True,
                "label_type": "key",
                "number_format": "SMART_NUMBER",
            }
        )
    elif viz_type in {"echarts_timeseries_bar", "echarts_timeseries_line"}:
        form_data.update(
            {
                "x_axis": spec.get("x_axis") or (groupby[0] if groupby else None),
                "groupby": [],
                "time_grain_sqla": "P1D",
                "show_legend": True,
                "rich_tooltip": True,
                "y_axis_format": "SMART_NUMBER",
            }
        )
    else:
        form_data.update(
            {
                "query_mode": "aggregate" if groupby or metrics else "raw",
                "groupby": groupby,
                "all_columns": spec.get("all_columns", []),
                "percent_metrics": [],
                "order_by_cols": [],
                "server_page_length": 10,
                "include_time": False,
                "show_cell_bars": True,
                "color_pn": True,
            }
        )
    return form_data


def chart_query_context(form_data: dict[str, Any], dataset_id: int) -> str:
    columns: list[Any] = []
    if form_data.get("viz_type") in {"echarts_timeseries_bar", "echarts_timeseries_line"} and form_data.get("x_axis"):
        columns = [form_data["x_axis"]]
    elif form_data.get("groupby"):
        columns = form_data["groupby"]
    elif form_data.get("query_mode") == "raw" and form_data.get("all_columns"):
        columns = form_data["all_columns"]
    metrics = form_data.get("metrics") or ([form_data["metric"]] if form_data.get("metric") else [])
    return safe_json_dumps(
        {
            "datasource": {"id": dataset_id, "type": "table"},
            "force": False,
            "queries": [
                {
                    "filters": [],
                    "extras": {"having": "", "where": ""},
                    "applied_time_extras": {},
                    "columns": columns,
                    "metrics": metrics,
                    "orderby": [],
                    "annotation_layers": [],
                    "row_limit": form_data.get("row_limit", 1000),
                    "series_limit": 0,
                    "order_desc": True,
                    "url_params": {},
                    "custom_params": {},
                    "custom_form_data": {},
                }
            ],
            "form_data": form_data,
            "result_format": "json",
            "result_type": "full",
        },
        sort_keys=True,
    )


def table_query_context(form_data: dict[str, Any], dataset_id: int) -> str:
    return chart_query_context(form_data, dataset_id)


def dynamic_chart_specs_for_dataset(dataset_name: str, columns: list[dict[str, str]]) -> list[dict[str, Any]]:
    count_column = preferred_count_column(columns)
    if not count_column:
        return []
    relation = dataset_relation(dataset_name)
    _, collection = dataset_parts(dataset_name)
    display_name = humanize_identifier(collection)
    count_metric = metric("Records", "COUNT", count_column)
    charts: list[dict[str, Any]] = [
        {
            "name": chart_name_for_dataset(dataset_name, "record_count"),
            "title": f"{display_name} Records",
            "dataset": dataset_name,
            "viz_type": "big_number_total",
            "metrics": [count_metric],
            "validation_sql": f"SELECT count({quote_identifier(count_column)}) AS records FROM {relation}",
        }
    ]

    temporal_column = preferred_temporal_column(columns)
    if temporal_column:
        expression = timestamp_sql_expression(temporal_column)
        charts.append(
            {
                "name": chart_name_for_dataset(dataset_name, f"{temporal_column['name']}_trend"),
                "title": f"{display_name} Over Time",
                "dataset": dataset_name,
                "viz_type": "echarts_timeseries_line",
                "x_axis": temporal_column["name"],
                "metrics": [count_metric],
                "validation_sql": (
                    f"SELECT date_trunc('day', {expression}) AS period, count({quote_identifier(count_column)}) AS records "
                    f"FROM {relation} WHERE {expression} IS NOT NULL GROUP BY 1 ORDER BY 1 LIMIT 1000"
                ),
            }
        )

    enum_column = preferred_enum_column(columns)
    if enum_column:
        quoted_enum = quote_identifier(enum_column["name"])
        charts.append(
            {
                "name": chart_name_for_dataset(dataset_name, f"{enum_column['name']}_breakdown"),
                "title": f"{display_name} by {humanize_identifier(enum_column['name'])}",
                "dataset": dataset_name,
                "viz_type": "echarts_pie",
                "groupby": [enum_column["name"]],
                "metrics": [count_metric],
                "row_limit": 25,
                "validation_sql": (
                    f"SELECT {quoted_enum}, count({quote_identifier(count_column)}) AS records "
                    f"FROM {relation} GROUP BY {quoted_enum} ORDER BY records DESC LIMIT 25"
                ),
            }
        )

    measure_column = preferred_measure_column(columns)
    if measure_column and len(charts) < BI_MAX_CHARTS_PER_DATASET:
        quoted_measure = quote_identifier(measure_column["name"])
        charts.append(
            {
                "name": chart_name_for_dataset(dataset_name, f"{measure_column['name']}_total"),
                "title": f"Total {humanize_identifier(measure_column['name'])}",
                "dataset": dataset_name,
                "viz_type": "big_number_total",
                "metrics": [metric(f"Total {humanize_identifier(measure_column['name'])}", "SUM", measure_column["name"])],
                "validation_sql": f"SELECT sum({quoted_measure}) AS total_value FROM {relation}",
            }
        )

    if len(charts) == 1:
        table_columns_for_chart = [column["name"] for column in columns[: min(len(columns), 8)]]
        charts.append(
            {
                "name": chart_name_for_dataset(dataset_name, "sample_records"),
                "title": f"{display_name} Sample Records",
                "dataset": dataset_name,
                "viz_type": "table",
                "all_columns": table_columns_for_chart,
                "row_limit": 25,
                "validation_sql": f"SELECT * FROM {relation} LIMIT 25",
            }
        )

    return charts[:BI_MAX_CHARTS_PER_DATASET]


def dynamic_dashboard_spec_for_dataset(dataset_name: str) -> dict[str, Any] | None:
    columns = table_column_details(dataset_name)
    charts = dynamic_chart_specs_for_dataset(dataset_name, columns)
    if not charts:
        return None
    database, collection = dataset_parts(dataset_name)
    dashboard_name = dashboard_name_for_dataset(dataset_name)
    return {
        "name": dashboard_name,
        "title": f"{humanize_identifier(database)} / {humanize_identifier(collection)}",
        "slug": slugify(dashboard_name),
        "dataset": dataset_name,
        "linked_datasets": [dataset_name],
        "columns": columns,
        "charts": charts,
    }


def dashboard_specs_for_datasets(dataset_names: set[str]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for dataset_name in sorted(dataset_names):
        try:
            spec = dynamic_dashboard_spec_for_dataset(dataset_name)
            if spec:
                specs.append(spec)
        except Exception:
            continue
    return specs


class SupersetClient:
    def __init__(self, base_url: str = SUPERSET_INTERNAL_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.authenticated = False

    def authenticate(self) -> None:
        if self.authenticated:
            return
        response = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={
                "username": SUPERSET_USERNAME,
                "password": SUPERSET_PASSWORD,
                "provider": "db",
                "refresh": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        token = response.json().get("access_token") or response.json().get("result", {}).get("access_token")
        if not token:
            raise RuntimeError("Superset login did not return an access token")
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.authenticated = True

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        self.authenticate()
        timeout = kwargs.pop("timeout", 60)
        if "json" in kwargs:
            original_payload = kwargs["json"]
            offenders = find_non_json_safe_fields(original_payload)
            if offenders:
                print(
                    safe_json_dumps(
                        {
                            "event": "superset_json_payload_non_json_values",
                            "method": method,
                            "path": path,
                            "fields": offenders[:50],
                        },
                        sort_keys=True,
                    )
                )
            kwargs["json"] = json_safe(original_payload)
            safe_json_dumps(kwargs["json"], sort_keys=True)
        response = None
        for attempt in range(4):
            response = self.session.request(method, f"{self.base_url}{path}", timeout=timeout, **kwargs)
            if response.status_code != 429:
                break
            time.sleep(1.1 * (attempt + 1))
        if response is None:
            raise RuntimeError("Superset request did not run")
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise requests.HTTPError(f"{exc}; response={response.text[:1000]}", response=response) from exc
        if response.status_code == 204 or not response.text:
            return {}
        return response.json()

    def get(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Any:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> Any:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Any:
        return self.request("DELETE", path, **kwargs)


def result_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return payload.get("result", payload)
    return payload


def result_id(payload: Any) -> int | None:
    result = result_payload(payload)
    if isinstance(result, dict):
        value = result.get("id")
        return int(value) if value is not None else None
    return None


def list_resource(client: SupersetClient, resource: str) -> list[dict[str, Any]]:
    payload = client.get(f"/api/v1/{resource}/", params={"q": safe_json_dumps({"page": 0, "page_size": 1000})})
    result = result_payload(payload)
    if isinstance(result, dict) and isinstance(result.get("data"), list):
        return result["data"]
    if isinstance(result, dict) and isinstance(result.get("result"), list):
        return result["result"]
    if isinstance(result, list):
        return result
    return []


def parse_json_field(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return value


def item_id(item: dict[str, Any]) -> int | None:
    value = item.get("id") or item.get("pk")
    return int(value) if value is not None else None


def superset_web_session() -> requests.Session:
    session = requests.Session()
    login_page = session.get(f"{SUPERSET_INTERNAL_URL}/login/", timeout=30)
    login_page.raise_for_status()
    csrf_match = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', login_page.text)
    payload = {
        "username": SUPERSET_USERNAME,
        "password": SUPERSET_PASSWORD,
    }
    if csrf_match:
        payload["csrf_token"] = csrf_match.group(1)
    response = session.post(f"{SUPERSET_INTERNAL_URL}/login/", data=payload, timeout=30, allow_redirects=True)
    response.raise_for_status()
    if "/login/" in response.url:
        raise RuntimeError("Superset web login did not complete")
    return session


def superset_http_status() -> dict[str, Any]:
    started = time.monotonic()
    try:
        response = requests.get(f"{SUPERSET_INTERNAL_URL}/health", timeout=8)
        return {
            "status": "ok" if response.ok else "error",
            "message": f"Superset HTTP {response.status_code}",
            "load_duration_ms": int((time.monotonic() - started) * 1000),
        }
    except Exception as exc:
        return {
            "status": "error",
            "message": str(exc),
            "load_duration_ms": int((time.monotonic() - started) * 1000),
        }


def superset_container_running() -> dict[str, Any]:
    docker = shutil.which("docker")
    if docker:
        try:
            result = subprocess.run(
                [docker, "inspect", "-f", "{{.State.Running}}", SUPERSET_CONTAINER_NAME],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                running = result.stdout.strip().lower() == "true"
                return {
                    "status": "ok" if running else "error",
                    "running": running,
                    "message": f"{SUPERSET_CONTAINER_NAME} running={running}",
                }
        except Exception as exc:
            return {"status": "unknown", "running": None, "message": str(exc)}
    health = superset_http_status()
    return {
        "status": health["status"],
        "running": health["status"] == "ok",
        "message": "Docker CLI unavailable; using Superset HTTP health as the container signal",
    }


def superset_health() -> dict[str, Any]:
    container = superset_container_running()
    http = superset_http_status()
    return {
        "status": "ok" if container.get("running") is True and http["status"] == "ok" else http["status"],
        "container": container,
        "http": http,
        "url": SUPERSET_EXTERNAL_URL,
        "checked_at": utc_now(),
    }


def upsert_bi_dataset(
    dataset_name: str,
    *,
    superset_dataset_id: int | None = None,
    row_count: int = 0,
    columns_count: int = 0,
    status: str,
    error_message: str | None = None,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bi_datasets (
                    dataset_name, source_safe_view, superset_dataset_id, row_count,
                    columns_count, status, last_refreshed, error_message, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, now(), %s, now())
                ON CONFLICT (dataset_name)
                DO UPDATE SET
                    source_safe_view = EXCLUDED.source_safe_view,
                    superset_dataset_id = COALESCE(EXCLUDED.superset_dataset_id, bi_datasets.superset_dataset_id),
                    row_count = EXCLUDED.row_count,
                    columns_count = EXCLUDED.columns_count,
                    status = EXCLUDED.status,
                    last_refreshed = EXCLUDED.last_refreshed,
                    error_message = EXCLUDED.error_message,
                    updated_at = now()
                """,
                (dataset_name, dataset_name, superset_dataset_id, row_count, columns_count, status, error_message),
            )


def upsert_bi_dashboard(
    dashboard_name: str,
    *,
    superset_dashboard_id: int | None = None,
    slug: str | None = None,
    chart_count: int = 0,
    linked_datasets: list[str] | None = None,
    generated_chart_names: list[str] | None = None,
    generation_time_ms: int | None = None,
    generated_by: str | None = BI_GENERATED_BY,
    status: str,
    refresh_status: str | None = None,
    load_duration_ms: int | None = None,
    error_message: str | None = None,
) -> None:
    init_dashboard_db()
    url_path = f"/superset/dashboard/{superset_dashboard_id}/" if superset_dashboard_id else None
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bi_dashboards (
                    dashboard_name, superset_dashboard_id, slug, url_path, chart_count,
                    linked_datasets_json, generated_chart_names_json, generation_time_ms, generated_by,
                    status, refresh_status, last_refresh_at, last_validation_at,
                    load_duration_ms, error_message, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s, %s, now())
                ON CONFLICT (dashboard_name)
                DO UPDATE SET
                    superset_dashboard_id = COALESCE(EXCLUDED.superset_dashboard_id, bi_dashboards.superset_dashboard_id),
                    slug = COALESCE(EXCLUDED.slug, bi_dashboards.slug),
                    url_path = COALESCE(EXCLUDED.url_path, bi_dashboards.url_path),
                    chart_count = EXCLUDED.chart_count,
                    linked_datasets_json = EXCLUDED.linked_datasets_json,
                    generated_chart_names_json = EXCLUDED.generated_chart_names_json,
                    generation_time_ms = EXCLUDED.generation_time_ms,
                    generated_by = EXCLUDED.generated_by,
                    status = EXCLUDED.status,
                    refresh_status = EXCLUDED.refresh_status,
                    last_refresh_at = EXCLUDED.last_refresh_at,
                    last_validation_at = EXCLUDED.last_validation_at,
                    load_duration_ms = EXCLUDED.load_duration_ms,
                    error_message = EXCLUDED.error_message,
                    updated_at = now()
                """,
                (
                    dashboard_name,
                    superset_dashboard_id,
                    slug,
                    url_path,
                    chart_count,
                    Json(json_safe(linked_datasets or [])),
                    Json(json_safe(generated_chart_names or [])),
                    generation_time_ms,
                    generated_by,
                    status,
                    refresh_status or status,
                    load_duration_ms,
                    error_message,
                ),
            )


def upsert_bi_chart(
    chart_name: str,
    *,
    superset_chart_id: int | None = None,
    dashboard_name: str | None,
    dataset_name: str,
    viz_type: str,
    status: str,
    generation_time_ms: int | None = None,
    error_message: str | None = None,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bi_charts (
                    chart_name, superset_chart_id, dashboard_name, dataset_name,
                    viz_type, status, generation_time_ms, error_message, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (chart_name)
                DO UPDATE SET
                    superset_chart_id = COALESCE(EXCLUDED.superset_chart_id, bi_charts.superset_chart_id),
                    dashboard_name = EXCLUDED.dashboard_name,
                    dataset_name = EXCLUDED.dataset_name,
                    viz_type = EXCLUDED.viz_type,
                    status = EXCLUDED.status,
                    generation_time_ms = EXCLUDED.generation_time_ms,
                    error_message = EXCLUDED.error_message,
                    updated_at = now()
                """,
                (chart_name, superset_chart_id, dashboard_name, dataset_name, viz_type, status, generation_time_ms, error_message),
            )


def record_dashboard_load(dashboard_name: str, status: str, load_duration_ms: int, error_message: str | None = None) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bi_dashboard_loads (id, dashboard_name, status, load_duration_ms, error_message)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (new_id(), dashboard_name, status, load_duration_ms, error_message),
            )
            cursor.execute(
                """
                UPDATE bi_dashboards
                SET last_viewed_at = now(),
                    status = %s,
                    load_duration_ms = %s,
                    error_message = %s,
                    updated_at = now()
                WHERE dashboard_name = %s
                """,
                (status, load_duration_ms, error_message, dashboard_name),
            )


def record_bi_query_failure(
    *,
    dashboard_name: str | None,
    dataset_name: str | None,
    chart_name: str | None,
    query_text: str | None,
    load_duration_ms: int,
    error_message: str,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bi_query_failures (
                    id, dashboard_name, dataset_name, chart_name, query_text,
                    load_duration_ms, error_message, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                """,
                (new_id(), dashboard_name, dataset_name, chart_name, query_text, load_duration_ms, error_message),
            )


def record_bi_validation_run(checks: list[dict[str, Any]], started_at: datetime, error_message: str | None = None) -> dict[str, Any]:
    passed = len([check for check in checks if check["status"] == "ok"])
    failed = len([check for check in checks if check["status"] == "failed"])
    warnings = len([check for check in checks if check["status"] == "warning"])
    status = "failed" if failed or error_message else "warning" if warnings else "ok"
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO bi_validation_runs (
                    id, status, started_at, finished_at, total_checks,
                    passed_checks, failed_checks, checks_json, error_message
                )
                VALUES (%s, %s, %s, now(), %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (new_id(), status, started_at, len(checks), passed, failed, Json(json_safe(checks)), error_message),
            )
            return as_dict(cursor.fetchone())


def check_result(
    name: str,
    ok: bool,
    message: str,
    details: dict[str, Any] | None = None,
    *,
    status: str | None = None,
) -> dict[str, Any]:
    return {"name": name, "status": status or ("ok" if ok else "failed"), "message": message, "details": details or {}}


def dataset_row(dataset_name: str) -> dict[str, Any]:
    columns = table_columns(dataset_name)
    return {
        "dataset_name": dataset_name,
        "columns": columns,
        "columns_count": len(columns),
        "row_count": table_row_count(dataset_name),
    }


def ensure_superset_database(client: SupersetClient) -> int:
    databases = list_resource(client, "database")
    existing = next((item for item in databases if item.get("database_name") == SUPERSET_DATABASE_NAME), None)
    payload = {
        "database_name": SUPERSET_DATABASE_NAME,
        "sqlalchemy_uri": SUPERSET_TRINO_URI,
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_file_upload": False,
        "extra": safe_json_dumps(
            {
                "metadata_params": {},
                "engine_params": {},
                "schemas_allowed_for_file_upload": [],
                "allows_virtual_table_explore": True,
            },
            sort_keys=True,
        ),
    }
    if existing:
        database_id = item_id(existing)
        if database_id is None:
            raise RuntimeError(f"Superset database exists without an id: {existing}")
        try:
            client.put(f"/api/v1/database/{database_id}", json=payload)
        except Exception:
            pass
        return database_id

    response = client.post("/api/v1/database/", json=payload)
    database_id = result_id(response)
    if database_id is None:
        databases = list_resource(client, "database")
        existing = next((item for item in databases if item.get("database_name") == SUPERSET_DATABASE_NAME), None)
        database_id = item_id(existing or {})
    if database_id is None:
        raise RuntimeError("Could not create or locate Superset Trino database")
    return database_id


def find_dataset(client: SupersetClient, dataset_name: str) -> dict[str, Any] | None:
    return next((item for item in list_resource(client, "dataset") if item.get("table_name") == dataset_name), None)


def ensure_superset_dataset(client: SupersetClient, database_id: int, dataset_name: str) -> int:
    dataset_sql = f"SELECT * FROM {dataset_relation(dataset_name)}"
    existing = find_dataset(client, dataset_name)
    if existing:
        dataset_id = item_id(existing)
        if dataset_id is None:
            raise RuntimeError(f"Superset dataset exists without an id: {existing}")
        try:
            client.put(
                f"/api/v1/dataset/{dataset_id}",
                json={"table_name": dataset_name, "schema": SUPERSET_SCHEMA, "sql": dataset_sql},
            )
        except Exception:
            pass
        try:
            client.put(f"/api/v1/dataset/{dataset_id}/refresh")
        except Exception:
            pass
        return dataset_id

    errors = []
    for payload in (
        {"database": database_id, "schema": SUPERSET_SCHEMA, "table_name": dataset_name, "sql": dataset_sql},
        {"database_id": database_id, "schema": SUPERSET_SCHEMA, "table_name": dataset_name, "sql": dataset_sql},
    ):
        try:
            response = client.post("/api/v1/dataset/", json=payload)
            dataset_id = result_id(response)
            if dataset_id is None:
                existing = find_dataset(client, dataset_name)
                dataset_id = item_id(existing or {})
            if dataset_id is not None:
                try:
                    client.put(f"/api/v1/dataset/{dataset_id}/refresh")
                except Exception:
                    pass
                return dataset_id
        except Exception as exc:
            errors.append(str(exc))
    raise RuntimeError(f"Could not create dataset {dataset_name}: {'; '.join(errors)}")


def find_dashboard(client: SupersetClient, name: str, slug: str, dashboards: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
    dashboards = dashboards if dashboards is not None else list_resource(client, "dashboard")
    return next(
        (
            item
            for item in dashboards
            if item.get("dashboard_title") == name or item.get("slug") == slug
        ),
        None,
    )


def ensure_superset_dashboard_record(client: SupersetClient, spec: dict[str, Any], dashboards: list[dict[str, Any]] | None = None) -> int:
    existing = find_dashboard(client, spec["name"], spec["slug"], dashboards)
    payload = {"dashboard_title": spec["name"], "slug": spec["slug"], "published": True}
    if existing:
        dashboard_id = item_id(existing)
        if dashboard_id is None:
            raise RuntimeError(f"Superset dashboard exists without an id: {existing}")
        try:
            client.put(f"/api/v1/dashboard/{dashboard_id}", json=payload)
        except Exception:
            pass
        return dashboard_id
    response = client.post("/api/v1/dashboard/", json=payload)
    dashboard_id = result_id(response)
    if dashboard_id is None:
        existing = find_dashboard(client, spec["name"], spec["slug"])
        dashboard_id = item_id(existing or {})
    if dashboard_id is None:
        raise RuntimeError(f"Could not create dashboard {spec['name']}")
    return dashboard_id


def chart_payload(spec: dict[str, Any], dataset_ids: dict[str, int], dashboard_id: int) -> dict[str, Any]:
    dataset_id = dataset_ids[spec["dataset"]]
    form_data = chart_form_data(spec, dataset_id)
    return {
        "slice_name": spec["name"],
        "viz_type": spec["viz_type"],
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": safe_json_dumps(form_data, sort_keys=True),
        "query_context": chart_query_context(form_data, dataset_id),
        "cache_timeout": None,
        "description": f"{BI_GENERATED_MARKER}: starter BI chart from safe view {spec['dataset']}",
        "dashboards": [dashboard_id],
    }


def find_chart(client: SupersetClient, chart_name: str, charts: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
    charts = charts if charts is not None else list_resource(client, "chart")
    return next((item for item in charts if item.get("slice_name") == chart_name), None)


def ensure_superset_chart(
    client: SupersetClient,
    spec: dict[str, Any],
    dataset_ids: dict[str, int],
    dashboard_id: int,
    charts: list[dict[str, Any]] | None = None,
) -> int:
    started = time.monotonic()
    payload = chart_payload(spec, dataset_ids, dashboard_id)
    try:
        existing = find_chart(client, spec["name"], charts)
        if existing:
            chart_id = item_id(existing)
            if chart_id is None:
                raise RuntimeError(f"Superset chart exists without an id: {existing}")
            client.put(f"/api/v1/chart/{chart_id}", json=payload)
        else:
            response = client.post("/api/v1/chart/", json=payload)
            chart_id = result_id(response)
            if chart_id is None:
                existing = find_chart(client, spec["name"])
                chart_id = item_id(existing or {})
            if chart_id is None:
                raise RuntimeError(f"Could not create chart {spec['name']}")
        upsert_bi_chart(
            spec["name"],
            superset_chart_id=chart_id,
            dashboard_name=None,
            dataset_name=spec["dataset"],
            viz_type=spec["viz_type"],
            status="ok",
            generation_time_ms=int((time.monotonic() - started) * 1000),
        )
        return chart_id
    except Exception as exc:
        upsert_bi_chart(
            spec["name"],
            dashboard_name=None,
            dataset_name=spec["dataset"],
            viz_type=spec["viz_type"],
            status="error",
            generation_time_ms=int((time.monotonic() - started) * 1000),
            error_message=str(exc),
        )
        raise


def dashboard_position(chart_ids: list[int]) -> str:
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"], "meta": {}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }
    for row_index in range(0, len(chart_ids), 2):
        row_id = f"ROW-{row_index // 2}"
        row_children = []
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": row_children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": SUPERSET_DASHBOARD_BACKGROUND},
        }
        for chart_id in chart_ids[row_index:row_index + 2]:
            component_id = f"CHART-{chart_id}"
            row_children.append(component_id)
            layout[component_id] = {
                "type": "CHART",
                "id": component_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": chart_id,
                    "height": 48,
                    "width": 6,
                    "background": SUPERSET_DASHBOARD_BACKGROUND,
                },
            }
    return safe_json_dumps(layout, sort_keys=True)


def dashboard_metadata(spec: dict[str, Any], chart_ids: list[int]) -> str:
    return safe_json_dumps(
        {
            "color_namespace": "onov8_enterprise",
            "label_colors": {},
            "refresh_frequency": 0,
            "timed_refresh_immune_slices": [],
            "expanded_slices": {str(chart_id): True for chart_id in chart_ids},
            "native_filter_configuration": [],
        },
        sort_keys=True,
    )


def update_superset_dashboard_layout(client: SupersetClient, dashboard_id: int, spec: dict[str, Any], chart_ids: list[int]) -> None:
    client.put(
        f"/api/v1/dashboard/{dashboard_id}",
        json={
            "dashboard_title": spec["name"],
            "slug": spec["slug"],
            "published": True,
            "position_json": dashboard_position(chart_ids),
            "json_metadata": dashboard_metadata(spec, chart_ids),
        },
    )


def generated_dashboard_registry() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM bi_dashboards WHERE generated_by = %s ORDER BY dashboard_name", (BI_GENERATED_BY,))
            return [as_dict(row) for row in cursor.fetchall()]


def generated_chart_registry() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM bi_charts ORDER BY chart_name")
            return [as_dict(row) for row in cursor.fetchall()]


def cleanup_generated_dashboards(client: SupersetClient, dashboard_names: set[str] | None = None) -> dict[str, Any]:
    deleted_dashboards = []
    deleted_charts = []
    errors = []
    registry_dashboards = [
        row
        for row in generated_dashboard_registry()
        if dashboard_names is None or row.get("dashboard_name") in dashboard_names
    ]
    registry_dashboard_names = {row["dashboard_name"] for row in registry_dashboards}
    registry_dashboard_ids = {int(row["superset_dashboard_id"]) for row in registry_dashboards if row.get("superset_dashboard_id")}
    registry_chart_rows = [
        row
        for row in generated_chart_registry()
        if dashboard_names is None or row.get("dashboard_name") in registry_dashboard_names
    ]
    registry_chart_ids = {int(row["superset_chart_id"]) for row in registry_chart_rows if row.get("superset_chart_id")}
    registry_chart_names = {row["chart_name"] for row in registry_chart_rows}

    for dashboard in list_resource(client, "dashboard"):
        dashboard_id = item_id(dashboard)
        title = dashboard.get("dashboard_title")
        if dashboard_id and (dashboard_id in registry_dashboard_ids or title in registry_dashboard_names):
            try:
                client.delete(f"/api/v1/dashboard/{dashboard_id}")
                deleted_dashboards.append({"id": dashboard_id, "dashboard_title": title, "slug": dashboard.get("slug")})
                time.sleep(0.03)
            except Exception as exc:
                errors.append({"resource": "dashboard", "id": dashboard_id, "name": title, "error": str(exc)})

    for chart in list_resource(client, "chart"):
        chart_id = item_id(chart)
        slice_name = chart.get("slice_name")
        if chart_id and (chart_id in registry_chart_ids or slice_name in registry_chart_names):
            try:
                client.delete(f"/api/v1/chart/{chart_id}")
                deleted_charts.append({"id": chart_id, "slice_name": slice_name})
                time.sleep(0.03)
            except Exception as exc:
                errors.append({"resource": "chart", "id": chart_id, "name": slice_name, "error": str(exc)})

    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            names = list(registry_dashboard_names)
            if names:
                cursor.execute("DELETE FROM bi_dashboard_loads WHERE dashboard_name = ANY(%s)", (names,))
                cursor.execute("DELETE FROM bi_query_failures WHERE dashboard_name = ANY(%s)", (names,))
                cursor.execute("DELETE FROM bi_charts WHERE dashboard_name = ANY(%s)", (names,))
                cursor.execute("DELETE FROM bi_dashboards WHERE dashboard_name = ANY(%s)", (names,))

    return {"deleted_dashboards": deleted_dashboards, "deleted_charts": deleted_charts, "errors": errors}


def ensure_superset_dashboards(
    client: SupersetClient,
    dataset_ids: dict[str, int],
    *,
    recreate: bool = False,
    new_only: bool = False,
    dashboard_specs: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    dashboard_specs = dashboard_specs or []
    if recreate:
        cleanup_generated_dashboards(client)
    if new_only:
        existing_names = {
            dashboard["dashboard_name"]
            for dashboard in fetch_bi_dashboards()
            if dashboard.get("status") == "ok" and dashboard.get("superset_dashboard_id")
        }
        dashboard_specs = [spec for spec in dashboard_specs if spec["name"] not in existing_names]
    dashboards = []
    superset_dashboards = list_resource(client, "dashboard")
    superset_charts = list_resource(client, "chart")
    for spec in dashboard_specs:
        started = time.monotonic()
        try:
            dashboard_id = ensure_superset_dashboard_record(client, spec, superset_dashboards)
            superset_dashboards = [
                item for item in superset_dashboards if item.get("dashboard_title") != spec["name"] and item.get("slug") != spec["slug"]
            ] + [{"id": dashboard_id, "dashboard_title": spec["name"], "slug": spec["slug"]}]
            chart_ids = []
            for chart in spec["charts"]:
                chart_id = ensure_superset_chart(client, chart, dataset_ids, dashboard_id, superset_charts)
                chart_ids.append(chart_id)
                superset_charts = [item for item in superset_charts if item.get("slice_name") != chart["name"]] + [
                    {"id": chart_id, "slice_name": chart["name"]}
                ]
            update_superset_dashboard_layout(client, dashboard_id, spec, chart_ids)
            generation_time_ms = int((time.monotonic() - started) * 1000)
            chart_names = [chart["name"] for chart in spec["charts"]]
            upsert_bi_dashboard(
                spec["name"],
                superset_dashboard_id=dashboard_id,
                slug=spec["slug"],
                chart_count=len(chart_ids),
                linked_datasets=spec.get("linked_datasets", []),
                generated_chart_names=chart_names,
                generation_time_ms=generation_time_ms,
                status="ok",
                refresh_status="ok",
            )
            for chart in spec["charts"]:
                upsert_bi_chart(
                    chart["name"],
                    dashboard_name=spec["name"],
                    dataset_name=chart["dataset"],
                    viz_type=chart["viz_type"],
                    status="ok",
                )
            dashboards.append(
                {
                    "dashboard_name": spec["name"],
                    "superset_dashboard_id": dashboard_id,
                    "slug": spec["slug"],
                    "chart_count": len(chart_ids),
                    "chart_ids": chart_ids,
                    "linked_datasets": spec.get("linked_datasets", []),
                    "generated_charts": chart_names,
                    "generation_time_ms": generation_time_ms,
                    "status": "ok",
                    "url": superset_url(f"/superset/dashboard/{dashboard_id}/"),
                }
            )
        except Exception as exc:
            generation_time_ms = int((time.monotonic() - started) * 1000)
            upsert_bi_dashboard(
                spec["name"],
                slug=spec["slug"],
                chart_count=len(spec["charts"]),
                linked_datasets=spec.get("linked_datasets", []),
                generated_chart_names=[chart["name"] for chart in spec["charts"]],
                generation_time_ms=generation_time_ms,
                status="error",
                refresh_status="error",
                error_message=str(exc),
            )
            dashboards.append(
                {
                    "dashboard_name": spec["name"],
                    "slug": spec["slug"],
                    "linked_datasets": spec.get("linked_datasets", []),
                    "generation_time_ms": generation_time_ms,
                    "status": "error",
                    "error_message": str(exc),
                }
            )
    return dashboards


def delete_generated_dashboards() -> dict[str, Any]:
    init_dashboard_db()
    client = SupersetClient()
    result = cleanup_generated_dashboards(client)
    return {
        "status": "ok" if not result.get("errors") else "error",
        "message": "Generated BI dashboards deleted" if not result.get("errors") else "Some generated BI dashboards could not be deleted",
        **result,
    }


def ensure_bi_layer(
    action: str = "metadata",
    *,
    recreate_dashboards: bool = False,
    new_dashboards_only: bool = False,
) -> dict[str, Any]:
    init_dashboard_db()
    started = time.monotonic()
    if action == "delete_dashboards":
        return delete_generated_dashboards()
    refresh_query_layer("metadata")
    client = SupersetClient()
    database_id = ensure_superset_database(client)
    dataset_ids: dict[str, int] = {}
    datasets = []
    safe_views = safe_views_detail()
    visible_views = {view["view_name"]: view for view in safe_views}
    eligibility = safe_view_eligibility_summary(safe_views)
    dataset_names = dynamic_safe_datasets()
    for dataset_name in dataset_names:
        try:
            view = visible_views.get(dataset_name)
            if not view or view.get("status") != "ok":
                raise RuntimeError(f"Safe view is not available: {dataset_name}")
            dataset_id = ensure_superset_dataset(client, database_id, dataset_name)
            row = dataset_row(dataset_name)
            upsert_bi_dataset(
                dataset_name,
                superset_dataset_id=dataset_id,
                row_count=row["row_count"],
                columns_count=row["columns_count"],
                status="ok",
            )
            dataset_ids[dataset_name] = dataset_id
            datasets.append({**row, "superset_dataset_id": dataset_id, "status": "ok"})
        except Exception as exc:
            upsert_bi_dataset(dataset_name, status="error", error_message=str(exc))
            datasets.append({"dataset_name": dataset_name, "status": "error", "error_message": str(exc)})
    dashboards = fetch_bi_dashboards()
    inventory = superset_inventory(client)
    failed_dataset_syncs = [dataset for dataset in datasets if dataset.get("status") != "ok"]
    return {
        "status": "ok" if not failed_dataset_syncs else "error",
        "message": "Superset BI metadata refreshed; eligible safe-view datasets synced",
        "action": action,
        "database_id": database_id,
        "safe_datasets": list(dataset_names),
        "eligibility": eligibility,
        "superset_inventory": inventory,
        "datasets": datasets,
        "dashboards": dashboards,
        "generated_dashboards": 0,
        "generated_charts": 0,
        "failed_dashboard_generations": 0,
        "dashboard_generation_status": "disabled",
        "dashboard_generation_message": DASHBOARD_GENERATION_DISABLED_MESSAGE,
        "generation_time_ms": int((time.monotonic() - started) * 1000),
    }


def dashboard_generation_disabled(action: str) -> dict[str, Any]:
    return {
        "status": "disabled",
        "action": action,
        "message": DASHBOARD_GENERATION_DISABLED_MESSAGE,
        "generated_dashboards": 0,
        "generated_charts": 0,
        "dashboards": fetch_bi_dashboards(),
    }


def fix_superset_dashboards() -> dict[str, Any]:
    return dashboard_generation_disabled("rebuild_dashboards")


def generate_new_superset_dashboards() -> dict[str, Any]:
    return dashboard_generation_disabled("new_dashboards_only")


def fetch_bi_datasets() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM bi_datasets ORDER BY dataset_name")
            rows = [as_dict(row) for row in cursor.fetchall()]
    for row in rows:
        database_name, collection_name = dataset_parts(row.get("dataset_name") or "")
        source_safe_view = row.get("source_safe_view") or row.get("dataset_name")
        probe = f"{row.get('dataset_name') or ''} {source_safe_view or ''}".lower()
        is_report = str(row.get("dataset_name") or "").startswith("report_")
        is_child = any(marker in probe for marker in EXPANDED_CHILD_DATASET_MARKERS)
        row["database_name"] = database_name
        row["collection_name"] = collection_name
        row["source_safe_view"] = source_safe_view
        row["safe_view_name"] = source_safe_view
        row["dataset_kind"] = "generated_report" if is_report else "expanded_child" if is_child else "parent_safe_view"
        row["dataset_label"] = (
            "Generated report dataset"
            if is_report
            else "Expanded child table"
            if is_child
            else "Parent safe analytical view"
        )
        row["row_multiplication_risk"] = bool(is_child)
        row["dataset_owner"] = "analytics-team"
        row["source_system"] = "trino_silver_safe_view"
        row["dataset_to_safe_view_mapping"] = f"{row.get('dataset_name')} -> {source_safe_view}"
    return rows


def fetch_bi_dashboards() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM bi_dashboards ORDER BY dashboard_name")
            rows = [as_dict(row) for row in cursor.fetchall()]
    for row in rows:
        row["linked_datasets"] = parse_json_field(row.get("linked_datasets_json"), [])
        row["generated_charts"] = parse_json_field(row.get("generated_chart_names_json"), [])
        if row.get("superset_dashboard_id"):
            row["url"] = superset_url(f"/superset/dashboard/{row['superset_dashboard_id']}/")
    return rows


def fetch_bi_charts() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM bi_charts ORDER BY dashboard_name NULLS LAST, chart_name")
            return [as_dict(row) for row in cursor.fetchall()]


def latest_bi_validation_run() -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM bi_validation_runs ORDER BY created_at DESC LIMIT 1")
            return as_dict(cursor.fetchone())


def fetch_bi_validation() -> dict[str, Any]:
    latest = latest_bi_validation_run()
    checks = latest.get("checks_json") or [] if latest else []
    return {"latest_run": latest, "checks": checks}


def bi_overview() -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT count(*) AS datasets_total FROM bi_datasets")
            datasets_total = int(cursor.fetchone()["datasets_total"] or 0)
            cursor.execute("SELECT count(*) AS datasets_count FROM bi_datasets WHERE status = 'ok'")
            datasets_count = int(cursor.fetchone()["datasets_count"] or 0)
            cursor.execute("SELECT count(*) AS failed_dataset_syncs FROM bi_datasets WHERE status <> 'ok'")
            failed_dataset_syncs = int(cursor.fetchone()["failed_dataset_syncs"] or 0)
            cursor.execute("SELECT count(*) AS dashboards_total FROM bi_dashboards")
            dashboards_total = int(cursor.fetchone()["dashboards_total"] or 0)
            cursor.execute("SELECT count(*) AS dashboards_count FROM bi_dashboards WHERE status = 'ok'")
            dashboards_count = int(cursor.fetchone()["dashboards_count"] or 0)
            cursor.execute("SELECT count(*) AS generated_dashboards_count FROM bi_dashboards WHERE generated_by = %s AND status = 'ok'", (BI_GENERATED_BY,))
            generated_dashboards_count = int(cursor.fetchone()["generated_dashboards_count"] or 0)
            cursor.execute("SELECT count(*) AS charts_total FROM bi_charts")
            charts_total = int(cursor.fetchone()["charts_total"] or 0)
            cursor.execute("SELECT count(*) AS generated_charts_count FROM bi_charts WHERE status = 'ok'")
            generated_charts_count = int(cursor.fetchone()["generated_charts_count"] or 0)
            cursor.execute("SELECT count(*) AS failed_dashboard_generations FROM bi_dashboards WHERE generated_by = %s AND status = 'error'", (BI_GENERATED_BY,))
            failed_dashboard_generations = int(cursor.fetchone()["failed_dashboard_generations"] or 0)
            cursor.execute("SELECT count(*) AS failed_chart_generations FROM bi_charts WHERE status = 'error'")
            failed_chart_generations = int(cursor.fetchone()["failed_chart_generations"] or 0)
            cursor.execute("SELECT max(last_refresh_at) AS last_dashboard_refresh FROM bi_dashboards")
            last_dashboard_refresh = cursor.fetchone()["last_dashboard_refresh"]
            cursor.execute("SELECT COALESCE(avg(generation_time_ms), 0) AS average_dashboard_generation_time_ms FROM bi_dashboards WHERE generated_by = %s", (BI_GENERATED_BY,))
            average_dashboard_generation_time_ms = float(cursor.fetchone()["average_dashboard_generation_time_ms"] or 0)
            cursor.execute("SELECT count(*) AS failed_dashboard_loads FROM bi_dashboard_loads WHERE status <> 'ok'")
            failed_dashboard_loads = int(cursor.fetchone()["failed_dashboard_loads"] or 0)
            cursor.execute("SELECT count(*) AS query_errors FROM bi_query_failures")
            query_errors = int(cursor.fetchone()["query_errors"] or 0)
            cursor.execute("SELECT COALESCE(avg(load_duration_ms), 0) AS average_dashboard_load_time FROM bi_dashboard_loads")
            average_dashboard_load_time = float(cursor.fetchone()["average_dashboard_load_time"] or 0)
    health = superset_health()
    latest_validation = latest_bi_validation_run()
    try:
        safe_views = safe_views_detail()
        eligibility = safe_view_eligibility_summary(safe_views)
    except Exception:
        safe_views = []
        eligibility = safe_view_eligibility_summary([])
    try:
        client = SupersetClient()
        inventory = superset_inventory(client)
    except Exception as exc:
        inventory = {
            "database_present": False,
            "superset_databases_count": 0,
            "superset_datasets_count": 0,
            "superset_charts_count": 0,
            "superset_dashboards_count": 0,
            "database_names": [],
            "dataset_names": [],
            "error_message": str(exc),
        }
    try:
        visibility = superset_visibility_report()
    except Exception as exc:
        visibility = {
            "visible_tables": [],
            "visible_tables_count": 0,
            "visible_analytics_tables_count": 0,
            "eligible_safe_views_count": eligibility["eligible_safe_views_count"],
            "unsafe_visible_views": [],
            "unsafe_visible_views_count": 0,
            "sample_view": None,
            "sample_rows": None,
            "base_table_access_blocked": True,
            "base_table_message": f"Visibility check failed: {exc}",
            "error_message": str(exc),
        }
    validation_status = (latest_validation or {}).get("status")
    sync_errors = failed_dataset_syncs + failed_dashboard_generations + failed_chart_generations
    unsafe_visible_count = int(visibility.get("unsafe_visible_views_count") or 0)
    readiness_reasons: list[str] = []
    if health.get("status") != "ok":
        readiness_reasons.append("superset_unreachable")
    if not inventory.get("database_present"):
        readiness_reasons.append("superset_database_missing")
    if eligibility["eligible_safe_views_count"] <= 0:
        readiness_reasons.append("no_eligible_safe_views")
    if datasets_count <= 0:
        readiness_reasons.append("no_synced_datasets")
    if unsafe_visible_count > 0:
        readiness_reasons.append("unsafe_views_visible_to_superset")
    if sync_errors > 0:
        readiness_reasons.append("sync_errors_present")
    if validation_status not in {"ok"}:
        readiness_reasons.append("validation_not_current")
    readiness_status = "ready" if not readiness_reasons else "not_ready"
    return {
        "status": readiness_status,
        "readiness_status": readiness_status,
        "readiness_reasons": readiness_reasons,
        "superset_status": health,
        "safe_views_available": eligibility["eligible_safe_views_count"],
        "safe_views_total": eligibility["total_safe_views"],
        "eligible_safe_views_count": eligibility["eligible_safe_views_count"],
        "non_ok_safe_views_count": eligibility["non_ok_safe_views_count"],
        "non_ok_safe_view_names": eligibility["non_ok_safe_view_names"],
        "ok_but_not_eligible_count": eligibility["ok_but_not_eligible_count"],
        "unsafe_visible_views_count": unsafe_visible_count,
        "unsafe_visible_views": visibility.get("unsafe_visible_views") or [],
        "superset_databases_count": inventory.get("superset_databases_count", 0),
        "superset_database_present": bool(inventory.get("database_present")),
        "superset_datasets_count": inventory.get("superset_datasets_count", 0),
        "superset_charts_count": inventory.get("superset_charts_count", 0),
        "superset_dashboards_count": inventory.get("superset_dashboards_count", 0),
        "superset_dataset_names": inventory.get("dataset_names") or [],
        "visibility": visibility,
        "datasets_total": datasets_total,
        "datasets_count": datasets_count,
        "failed_dataset_syncs": failed_dataset_syncs,
        "dashboards_total": dashboards_total,
        "dashboards_count": dashboards_count,
        "charts_total": charts_total,
        "generated_dashboards_count": generated_dashboards_count,
        "generated_charts_count": generated_charts_count,
        "failed_dashboard_generations": failed_dashboard_generations,
        "failed_chart_generations": failed_chart_generations,
        "dashboard_generation_status": "disabled",
        "dashboard_generation_message": DASHBOARD_GENERATION_DISABLED_MESSAGE,
        "last_dashboard_refresh": last_dashboard_refresh,
        "average_dashboard_generation_time_ms": average_dashboard_generation_time_ms,
        "failed_dashboard_loads": failed_dashboard_loads,
        "query_errors": query_errors,
        "average_dashboard_load_time": average_dashboard_load_time,
        "latest_validation": latest_validation,
    }


def pii_columns_for_view(view_name: str) -> list[str]:
    blocked = []
    for column in table_columns(view_name):
        if is_pii_column(column):
            blocked.append(column)
    return blocked


def superset_inventory(client: SupersetClient | None = None) -> dict[str, Any]:
    local_client = client or SupersetClient()
    databases = list_resource(local_client, "database")
    datasets = list_resource(local_client, "dataset")
    charts = list_resource(local_client, "chart")
    dashboards = list_resource(local_client, "dashboard")
    database_present = any(item.get("database_name") == SUPERSET_DATABASE_NAME for item in databases)
    return {
        "database_present": database_present,
        "superset_databases_count": len(databases),
        "superset_datasets_count": len(datasets),
        "superset_charts_count": len(charts),
        "superset_dashboards_count": len(dashboards),
        "database_names": sorted(str(item.get("database_name") or "") for item in databases if item.get("database_name")),
        "dataset_names": sorted(str(item.get("table_name") or "") for item in datasets if item.get("table_name")),
    }


def superset_visibility_report() -> dict[str, Any]:
    connection = trino_connection(schema=SUPERSET_SCHEMA, user=SUPERSET_TRINO_USER)
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW TABLES FROM delta.silver")
        visible = sorted(str(row[0]) for row in cursor.fetchall())
        eligible = set(dynamic_safe_datasets())
        unsafe = sorted(
            table
            for table in visible
            if table.endswith("_analytics") and table not in eligible and not table.startswith("information_schema")
        )
        sample_view = sorted(eligible)[0] if eligible else None
        sample_rows = None
        base_table_access_blocked = True
        base_table_message = "No eligible safe views are visible to Superset"
        if sample_view:
            cursor.execute(f'SELECT count(*) FROM delta.silver."{sample_view}"')
            sample_rows = int(cursor.fetchone()[0])
            source_table = sample_view.removesuffix("_analytics") + "_clean"
            try:
                cursor.execute(f'SELECT * FROM delta.silver."{source_table}" LIMIT 1')
                base_table_access_blocked = False
                base_table_message = f"Superset Trino user was able to query base Silver table {source_table}"
            except Exception:
                base_table_access_blocked = True
                base_table_message = "Superset Trino user cannot query base Silver tables"
        return {
            "visible_tables": visible,
            "visible_tables_count": len(visible),
            "visible_analytics_tables_count": len([table for table in visible if table.endswith("_analytics")]),
            "eligible_safe_views_count": len(eligible),
            "unsafe_visible_views": unsafe,
            "unsafe_visible_views_count": len(unsafe),
            "sample_view": sample_view,
            "sample_rows": sample_rows,
            "base_table_access_blocked": base_table_access_blocked,
            "base_table_message": base_table_message,
        }
    finally:
        cursor.close()
        connection.close()


def validate_superset_trino_user() -> tuple[bool, str, dict[str, Any]]:
    details = superset_visibility_report()
    unsafe = details.get("unsafe_visible_views") or []
    if unsafe:
        return False, f"Superset Trino user can see unsafe tables: {', '.join(unsafe)}", details
    if details.get("eligible_safe_views_count", 0) <= 0:
        return False, "No eligible safe datasets are visible to Superset", details
    if not details.get("base_table_access_blocked"):
        return False, details.get("base_table_message", "Superset Trino user can query base Silver tables"), details
    sample_view = details.get("sample_view")
    sample_rows = details.get("sample_rows")
    return True, f"Superset Trino user can query eligible safe views only; {sample_view} rows={sample_rows}", details


def load_dashboard(client: SupersetClient, dashboard: dict[str, Any]) -> dict[str, Any]:
    dashboard_id = dashboard.get("superset_dashboard_id")
    if not dashboard_id:
        return {"status": "failed", "message": "Dashboard has no Superset id", "load_duration_ms": 0, "chart_count": 0}
    started = time.monotonic()
    try:
        detail = client.get(f"/api/v1/dashboard/{dashboard_id}")
        charts = client.get(f"/api/v1/dashboard/{dashboard_id}/charts")
        duration_ms = int((time.monotonic() - started) * 1000)
        chart_items = result_payload(charts)
        if isinstance(chart_items, dict) and isinstance(chart_items.get("result"), list):
            chart_items = chart_items["result"]
        if isinstance(chart_items, dict) and isinstance(chart_items.get("charts"), list):
            chart_items = chart_items["charts"]
        chart_count = len(chart_items) if isinstance(chart_items, list) else int(dashboard.get("chart_count") or 0)
        status = "slow" if duration_ms >= DASHBOARD_LOAD_THRESHOLD_MS else "ok"
        message = f"Dashboard API loaded in {duration_ms} ms"
        record_dashboard_load(dashboard["dashboard_name"], "ok" if status == "slow" else status, duration_ms)
        return {"status": status, "message": message, "load_duration_ms": duration_ms, "chart_count": chart_count, "detail": result_payload(detail)}
    except Exception as exc:
        duration_ms = int((time.monotonic() - started) * 1000)
        record_dashboard_load(dashboard["dashboard_name"], "failed", duration_ms, str(exc))
        return {"status": "failed", "message": str(exc), "load_duration_ms": duration_ms, "chart_count": 0}


def sql_safety_check(sql_text: str, dataset_name: str) -> tuple[bool, str]:
    text = (sql_text or "").strip()
    lowered = f" {text.lower()} "
    disallowed = (" insert ", " update ", " delete ", " drop ", " alter ", " create ", " merge ", " truncate ")
    if not text:
        return False, "Dataset SQL is empty"
    if not lowered.lstrip().startswith(" select "):
        return False, "Dataset SQL must be SELECT-only"
    if any(token in lowered for token in disallowed):
        return False, "Dataset SQL contains disallowed write/DDL keywords"
    if "_clean" in lowered:
        return False, "Dataset SQL references base clean tables"
    quoted_name = f'"{dataset_name.lower()}"'
    if dataset_name.lower() not in lowered and quoted_name not in lowered:
        return False, "Dataset SQL does not reference the expected safe analytics view"
    if "delta.silver" not in lowered:
        return False, "Dataset SQL does not reference delta.silver safe views"
    return True, "Dataset SQL is SELECT-only and scoped to safe analytics views"


def validate_chart_queries(dashboard_specs: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    checks = []
    for dashboard in dashboard_specs or dashboard_specs_for_datasets(set(dynamic_safe_datasets())):
        for chart in dashboard["charts"]:
            started = time.monotonic()
            sql = chart["validation_sql"]
            try:
                columns, rows, _ = trino_query(
                    sql,
                    observe=True,
                    user_source="superset_bi_validation",
                    selected_view=chart["dataset"],
                    trino_user=SUPERSET_TRINO_USER,
                )
                duration_ms = int((time.monotonic() - started) * 1000)
                checks.append(
                    check_result(
                        f"{chart['name']}_query_succeeds",
                        True,
                        f"{len(rows)} rows in {duration_ms} ms",
                        {"dashboard": dashboard["name"], "dataset": chart["dataset"], "columns": columns, "duration_ms": duration_ms},
                    )
                )
            except Exception as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                record_bi_query_failure(
                    dashboard_name=dashboard["name"],
                    dataset_name=chart["dataset"],
                    chart_name=chart["name"],
                    query_text=sql,
                    load_duration_ms=duration_ms,
                    error_message=str(exc),
                )
                checks.append(check_result(f"{chart['name']}_query_succeeds", False, str(exc), {"dashboard": dashboard["name"], "dataset": chart["dataset"]}))
    return checks


def validate_position_json(dashboard_name: str, position_json: Any, expected_chart_count: int) -> list[dict[str, Any]]:
    layout = parse_json_field(position_json, {})
    checks = [
        check_result(
            f"{dashboard_name}_position_json_valid",
            isinstance(layout, dict) and layout.get("DASHBOARD_VERSION_KEY") == "v2",
            "Dashboard layout is v2 JSON",
        )
    ]
    if not isinstance(layout, dict):
        return checks

    missing_meta = [
        component_id
        for component_id, component in layout.items()
        if component_id != "DASHBOARD_VERSION_KEY"
        and isinstance(component, dict)
        and "meta" not in component
    ]
    chart_components = [
        component
        for component in layout.values()
        if isinstance(component, dict) and component.get("type") == "CHART"
    ]
    broken_chart_components = [
        component.get("id")
        for component in chart_components
        if not isinstance(component.get("meta"), dict)
        or not component["meta"].get("chartId")
        or not component["meta"].get("height")
        or not component["meta"].get("width")
        or "background" not in component["meta"]
    ]
    checks.append(
        check_result(
            f"{dashboard_name}_layout_components_have_meta",
            not missing_meta,
            "All dashboard layout components include meta",
            {"missing_meta": missing_meta},
        )
    )
    checks.append(
        check_result(
            f"{dashboard_name}_chart_layout_valid",
            len(chart_components) >= expected_chart_count and not broken_chart_components,
            f"{len(chart_components)} chart layout components",
            {"broken_chart_components": broken_chart_components},
        )
    )
    return checks


def validate_dashboard_metadata(client: SupersetClient, dashboard_specs: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    specs = dashboard_specs if dashboard_specs is not None else dashboard_specs_for_datasets(set(dynamic_safe_datasets()))
    if not specs:
        return [check_result("dynamic_starter_dashboards_applicable", False, "No dashboard specs could be inferred from the currently discovered safe views")]
    dashboards = list_resource(client, "dashboard")
    expected_names = {dashboard["name"] for dashboard in specs}
    found = {dashboard.get("dashboard_title") for dashboard in dashboards if dashboard.get("dashboard_title") in expected_names}
    checks.append(
        check_result(
            "superset_dashboard_list_contains_dynamic_dashboards",
            expected_names <= found,
            f"Found dashboards: {', '.join(sorted(found))}",
            {"expected": sorted(expected_names), "found": sorted(found)},
        )
    )

    web_session: requests.Session | None = None
    try:
        web_session = superset_web_session()
    except Exception as exc:
        checks.append(check_result("superset_web_login", False, str(exc)))

    for spec in specs:
        dashboard = next(
            (
                row
                for row in dashboards
                if row.get("dashboard_title") == spec["name"] or row.get("slug") == spec["slug"]
            ),
            None,
        )
        dashboard_id = item_id(dashboard or {})
        checks.append(check_result(f"{spec['name']}_exists", dashboard_id is not None, "Dashboard exists in Superset"))
        if dashboard_id is None:
            continue

        detail_payload = client.get(f"/api/v1/dashboard/{dashboard_id}")
        detail = result_payload(detail_payload)
        if isinstance(detail, dict) and "result" in detail:
            detail = detail["result"]
        if not isinstance(detail, dict):
            detail = {}

        charts = detail.get("charts") or []
        owners = detail.get("owners") or []
        metadata = parse_json_field(detail.get("json_metadata"), {})
        checks.append(
            check_result(
                f"{spec['name']}_published",
                bool(detail.get("published")),
                "Dashboard is published",
            )
        )
        checks.append(
            check_result(
                f"{spec['name']}_owners_valid",
                bool(owners),
                "Dashboard has owners",
                {"owners": owners},
            )
        )
        checks.append(
            check_result(
                f"{spec['name']}_charts_attached",
                len(charts) >= len(spec["charts"]),
                f"{len(charts)} charts attached",
                {"charts": charts},
            )
        )
        checks.append(
            check_result(
                f"{spec['name']}_json_metadata_valid",
                isinstance(metadata, dict),
                "Dashboard json_metadata is valid JSON",
            )
        )
        checks.extend(validate_position_json(spec["name"], detail.get("position_json"), len(spec["charts"])))

        if web_session is not None:
            try:
                response = web_session.get(f"{SUPERSET_INTERNAL_URL}/superset/dashboard/{dashboard_id}/", timeout=30)
                page_ok = (
                    response.ok
                    and "/login/" not in response.url
                    and spec["name"] in response.text
                    and "Unexpected error" not in response.text
                    and "undefined is not an object" not in response.text
                    and "Cannot read properties" not in response.text
                    and "TypeError" not in response.text
                )
                checks.append(
                    check_result(
                        f"{spec['name']}_dashboard_page_returns",
                        page_ok,
                        f"Dashboard page HTTP {response.status_code}",
                        {"url": response.url, "html_size": len(response.text)},
                    )
                )
            except Exception as exc:
                checks.append(check_result(f"{spec['name']}_dashboard_page_returns", False, str(exc)))
    return checks


def validate_chart_metadata(client: SupersetClient, dashboard_specs: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    specs = dashboard_specs if dashboard_specs is not None else dashboard_specs_for_datasets(set(dynamic_safe_datasets()))
    expected_chart_names = {chart["name"] for dashboard in specs for chart in dashboard["charts"]}
    if not expected_chart_names:
        return [check_result("dynamic_starter_charts_applicable", False, "No chart specs could be inferred from the currently discovered safe views")]
    charts = list_resource(client, "chart")
    found = {chart.get("slice_name") for chart in charts if chart.get("slice_name") in expected_chart_names}
    checks.append(
        check_result(
            "superset_chart_list_contains_dynamic_charts",
            expected_chart_names <= found,
            f"Found charts: {len(found)}",
            {"expected": sorted(expected_chart_names), "found": sorted(found)},
        )
    )

    for chart in charts:
        chart_id = item_id(chart)
        if chart_id is None or chart.get("slice_name") not in expected_chart_names:
            continue
        detail_payload = client.get(f"/api/v1/chart/{chart_id}")
        detail = result_payload(detail_payload)
        if isinstance(detail, dict) and "result" in detail:
            detail = detail["result"]
        if not isinstance(detail, dict):
            detail = {}

        params = parse_json_field(detail.get("params"), {})
        query_context = parse_json_field(detail.get("query_context"), {})
        owners = detail.get("owners") or []
        metadata_ok = (
            bool(detail.get("slice_name"))
            and detail.get("viz_type") in BI_SUPPORTED_VIZ_TYPES
            and bool(params.get("datasource"))
            and params.get("viz_type") == detail.get("viz_type")
            and isinstance(query_context, dict)
            and bool(query_context.get("queries"))
            and bool(owners)
        )
        checks.append(
            check_result(
                f"{detail.get('slice_name', chart_id)}_chart_metadata_valid",
                metadata_ok,
                "Chart metadata is complete",
                {
                    "chart_id": chart_id,
                    "viz_type": detail.get("viz_type"),
                    "has_query_context": isinstance(query_context, dict) and bool(query_context.get("queries")),
                    "owner_count": len(owners),
                },
            )
        )
        if isinstance(query_context, dict) and query_context.get("queries"):
            try:
                response = client.post("/api/v1/chart/data", json=query_context, timeout=120)
                result = result_payload(response)
                if isinstance(result, dict) and isinstance(result.get("result"), list):
                    result_items = result["result"]
                elif isinstance(result, list):
                    result_items = result
                else:
                    result_items = []
                failed_items = [item for item in result_items if item.get("status") not in {None, "success"} or item.get("error")]
                checks.append(
                    check_result(
                        f"{detail.get('slice_name', chart_id)}_superset_chart_data_succeeds",
                        not failed_items and bool(result_items),
                        "Superset chart data endpoint succeeds",
                        {"failed_items": failed_items, "result_count": len(result_items)},
                    )
                )
            except Exception as exc:
                checks.append(check_result(f"{detail.get('slice_name', chart_id)}_superset_chart_data_succeeds", False, str(exc)))
    return checks


def validate_bi_layer(record: bool = True) -> dict[str, Any]:
    init_dashboard_db()
    started_at = utc_now()
    checks: list[dict[str, Any]] = []
    error_message = None
    try:
        health = superset_health()
        checks.append(check_result("superset_container_running", health["container"].get("running") is True, health["container"].get("message", ""), health["container"]))
        checks.append(check_result("superset_reachable", health["http"].get("status") == "ok", health["http"].get("message", ""), health["http"]))

        safe_views = safe_views_detail()
        eligibility = safe_view_eligibility_summary(safe_views)
        expected_datasets = set(eligibility["eligible_safe_view_names"])
        safe_view_names = {view["view_name"] for view in safe_views if view.get("status") == "ok"}
        checks.append(
            check_result(
                "eligible_safe_views_available",
                eligibility["eligible_safe_views_count"] > 0,
                f"Eligible safe views: {eligibility['eligible_safe_views_count']}",
                eligibility,
            )
        )
        checks.append(check_result("safe_views_visible", bool(expected_datasets) and expected_datasets <= safe_view_names, f"Safe views: {', '.join(sorted(safe_view_names))}", {"expected": sorted(expected_datasets)}))

        superset_user_ok, superset_user_message, superset_user_details = validate_superset_trino_user()
        checks.append(check_result("trino_connection_from_superset_user", superset_user_ok, superset_user_message, superset_user_details))
        checks.append(
            check_result(
                "no_unsafe_visible_views_for_superset",
                not bool((superset_user_details or {}).get("unsafe_visible_views")),
                "Superset user cannot see non-eligible safe views"
                if not bool((superset_user_details or {}).get("unsafe_visible_views"))
                else f"Unsafe visible views: {', '.join((superset_user_details or {}).get('unsafe_visible_views') or [])}",
                {"unsafe_visible_views": (superset_user_details or {}).get("unsafe_visible_views") or []},
            )
        )

        client = SupersetClient()
        inventory = superset_inventory(client)
        checks.append(
            check_result(
                "superset_database_present",
                bool(inventory.get("database_present")),
                f"Superset database '{SUPERSET_DATABASE_NAME}' is {'present' if inventory.get('database_present') else 'missing'}",
                inventory,
            )
        )
        checks.append(
            check_result(
                "superset_datasets_present",
                int(inventory.get("superset_datasets_count") or 0) > 0,
                f"Superset datasets: {inventory.get('superset_datasets_count', 0)}",
                inventory,
            )
        )

        for view_name in sorted(expected_datasets):
            blocked = pii_columns_for_view(view_name)
            checks.append(check_result(f"{view_name}_pii_safe", not blocked, "No raw PII columns exposed" if not blocked else f"Blocked PII columns visible: {blocked}", {"blocked_columns": blocked}))

        datasets = fetch_bi_datasets()
        stale_datasets = [dataset for dataset in datasets if dataset.get("dataset_name") not in expected_datasets]
        broken_datasets = [dataset for dataset in datasets if dataset.get("status") not in {"ok"}]
        checks.append(
            check_result(
                "console_dataset_registry_present",
                len(datasets) > 0,
                f"Console BI datasets: {len(datasets)}",
                {"dataset_count": len(datasets)},
            )
        )
        checks.append(
            check_result(
                "no_stale_console_datasets",
                not stale_datasets,
                "No stale BI datasets outside eligible safe views" if not stale_datasets else f"Stale datasets: {', '.join(sorted(dataset.get('dataset_name') for dataset in stale_datasets if dataset.get('dataset_name')))}",
                {"stale_dataset_count": len(stale_datasets)},
                status="warning" if stale_datasets else "ok",
            )
        )
        checks.append(
            check_result(
                "no_broken_console_datasets",
                not broken_datasets,
                "No broken BI datasets" if not broken_datasets else f"Broken datasets: {', '.join(sorted(dataset.get('dataset_name') for dataset in broken_datasets if dataset.get('dataset_name')))}",
                {"broken_dataset_count": len(broken_datasets)},
                status="warning" if broken_datasets else "ok",
            )
        )
        dataset_names = {dataset["dataset_name"] for dataset in datasets if dataset.get("status") == "ok" and dataset.get("superset_dataset_id")}
        checks.append(check_result("superset_datasets_exist", expected_datasets <= dataset_names, f"Datasets: {', '.join(sorted(dataset_names))}", {"expected": sorted(expected_datasets)}))
        dataset_resources = list_resource(client, "dataset")
        resource_by_name = {
            str(item.get("table_name")): item for item in dataset_resources if item.get("table_name")
        }
        for dataset_name in sorted(expected_datasets):
            resource = resource_by_name.get(dataset_name)
            resource_id = item_id(resource or {})
            checks.append(
                check_result(
                    f"{dataset_name}_superset_dataset_registered",
                    resource_id is not None,
                    "Superset dataset exists" if resource_id is not None else "Superset dataset is missing",
                    {"dataset_name": dataset_name, "superset_dataset_id": resource_id},
                )
            )
            if resource_id is None:
                continue
            detail_payload = client.get(f"/api/v1/dataset/{resource_id}")
            detail = result_payload(detail_payload)
            if isinstance(detail, dict) and "result" in detail:
                detail = detail["result"]
            if not isinstance(detail, dict):
                detail = {}
            sql_ok, sql_message = sql_safety_check(str(detail.get("sql") or ""), dataset_name)
            checks.append(
                check_result(
                    f"{dataset_name}_dataset_sql_safety",
                    sql_ok,
                    sql_message,
                    {"dataset_name": dataset_name, "superset_dataset_id": resource_id},
                )
            )
        checks.append(
            check_result(
                "automatic_dashboard_generation_disabled",
                True,
                DASHBOARD_GENERATION_DISABLED_MESSAGE,
                {"generated_by": BI_GENERATED_BY},
            )
        )

        dashboards = fetch_bi_dashboards()
        stale_dashboards = [dashboard for dashboard in dashboards if dashboard.get("status") not in {"ok", "slow", "unknown"}]
        checks.append(
            check_result(
                "dashboard_registry_consistent",
                not stale_dashboards,
                "No broken dashboard registry rows"
                if not stale_dashboards
                else f"Broken dashboards: {', '.join(sorted(dashboard.get('dashboard_name') for dashboard in stale_dashboards if dashboard.get('dashboard_name')))}",
                {"broken_dashboard_count": len(stale_dashboards)},
                status="warning" if stale_dashboards else "ok",
            )
        )
        if dashboards:
            manual_dashboards = [dashboard for dashboard in dashboards if dashboard.get("generated_by") != BI_GENERATED_BY]
            checks.append(
                check_result(
                    "manual_dashboards_discovered",
                    True,
                    f"Manual dashboards discovered: {len(manual_dashboards)}",
                    {"manual_dashboards": [dashboard.get("dashboard_name") for dashboard in manual_dashboards]},
                    status="ok",
                )
            )
        for dashboard in dashboards:
            load = load_dashboard(client, dashboard)
            checks.append(
                check_result(
                    f"{dashboard['dashboard_name']}_loads",
                    load["status"] in {"ok", "slow"},
                    load["message"],
                    load,
                )
            )
            checks.append(
                check_result(
                    f"{dashboard['dashboard_name']}_charts_registered",
                    int(load.get("chart_count") or 0) >= int(dashboard.get("chart_count") or 0),
                    f"{load.get('chart_count', 0)} charts visible",
                    load,
                )
            )
            if dashboard.get("superset_dashboard_id"):
                try:
                    detail_payload = client.get(f"/api/v1/dashboard/{dashboard['superset_dashboard_id']}")
                    detail = result_payload(detail_payload)
                    if isinstance(detail, dict) and "result" in detail:
                        detail = detail["result"]
                    if not isinstance(detail, dict):
                        detail = {}
                    dashboard_meta_ok = bool(detail.get("dashboard_title")) and isinstance(parse_json_field(detail.get("position_json"), {}), dict)
                    checks.append(
                        check_result(
                            f"{dashboard['dashboard_name']}_metadata_valid",
                            dashboard_meta_ok,
                            "Dashboard metadata is valid" if dashboard_meta_ok else "Dashboard metadata is incomplete",
                            {"dashboard_id": dashboard.get("superset_dashboard_id")},
                            status="warning" if not dashboard_meta_ok else "ok",
                        )
                    )
                except Exception as exc:
                    checks.append(
                        check_result(
                            f"{dashboard['dashboard_name']}_metadata_valid",
                            False,
                            str(exc),
                            status="warning",
                        )
                    )

        charts = fetch_bi_charts()
        broken_charts = [chart for chart in charts if chart.get("status") not in {"ok", "unknown"}]
        checks.append(
            check_result(
                "chart_registry_consistent",
                not broken_charts,
                "No broken chart registry rows"
                if not broken_charts
                else f"Broken charts: {', '.join(sorted(chart.get('chart_name') for chart in broken_charts if chart.get('chart_name')))}",
                {"broken_chart_count": len(broken_charts)},
                status="warning" if broken_charts else "ok",
            )
        )
        if charts:
            for chart in charts:
                superset_chart_id = chart.get("superset_chart_id")
                if not superset_chart_id:
                    checks.append(
                        check_result(
                            f"{chart.get('chart_name', 'chart')}_metadata_valid",
                            False,
                            "Chart has no Superset chart id",
                            {"chart_name": chart.get("chart_name")},
                            status="warning",
                        )
                    )
                    continue
                try:
                    detail_payload = client.get(f"/api/v1/chart/{superset_chart_id}")
                    detail = result_payload(detail_payload)
                    if isinstance(detail, dict) and "result" in detail:
                        detail = detail["result"]
                    if not isinstance(detail, dict):
                        detail = {}
                    chart_ok = bool(detail.get("slice_name")) and bool(detail.get("datasource_id"))
                    checks.append(
                        check_result(
                            f"{chart.get('chart_name', superset_chart_id)}_metadata_valid",
                            chart_ok,
                            "Chart metadata is valid" if chart_ok else "Chart metadata is incomplete",
                            {"chart_id": superset_chart_id},
                            status="warning" if not chart_ok else "ok",
                        )
                    )
                except Exception as exc:
                    checks.append(
                        check_result(
                            f"{chart.get('chart_name', superset_chart_id)}_metadata_valid",
                            False,
                            str(exc),
                            status="warning",
                        )
                    )
        else:
            checks.append(
                check_result(
                    "charts_discovered",
                    True,
                    "No charts are registered yet; this is acceptable while dashboards remain manual",
                    {"chart_count": 0},
                    status="ok",
                )
            )
    except Exception as exc:
        error_message = str(exc)
        checks.append(check_result("bi_validation_exception", False, error_message))

    run = record_bi_validation_run(checks, started_at, error_message) if record else {"status": "ok" if all(check["status"] == "ok" for check in checks) else "failed"}
    return {"status": run["status"], "run": run, "checks": checks}


def validate_bi_dashboards(record: bool = True) -> dict[str, Any]:
    init_dashboard_db()
    started_at = utc_now()
    checks: list[dict[str, Any]] = []
    error_message = None
    try:
        client = SupersetClient()
        checks.append(check_result("automatic_dashboard_generation_disabled", True, DASHBOARD_GENERATION_DISABLED_MESSAGE))
        dashboards = fetch_bi_dashboards()
        checks.append(
            check_result(
                "dashboard_registry_available",
                True,
                f"Dashboard rows discovered: {len(dashboards)}",
                {"dashboard_count": len(dashboards)},
            )
        )
        for dashboard in dashboards:
            load = load_dashboard(client, dashboard)
            checks.append(check_result(f"{dashboard['dashboard_name']}_loads", load["status"] in {"ok", "slow"}, load["message"], load))
    except Exception as exc:
        error_message = str(exc)
        checks.append(check_result("bi_dashboard_validation_exception", False, error_message))
    run = record_bi_validation_run(checks, started_at, error_message) if record else {"status": "ok" if all(check["status"] == "ok" for check in checks) else "failed"}
    return {"status": run["status"], "run": run, "checks": checks}


def validate_trino_views_task() -> dict[str, Any]:
    refresh_query_layer("metadata")
    views = safe_views_detail()
    visible = {view["view_name"] for view in eligible_safe_views(views)}
    if not visible:
        raise RuntimeError("No eligible safe BI views are visible")
    return {"status": "ok", "safe_views": views}


def validate_superset_connection_task() -> dict[str, Any]:
    health = superset_health()
    if health["http"]["status"] != "ok":
        raise RuntimeError(health["http"]["message"])
    superset_user_ok, message, details = validate_superset_trino_user()
    if not superset_user_ok:
        raise RuntimeError(message)
    return {"status": "ok", "health": health, "message": message, "details": details}


def validate_datasets_task() -> dict[str, Any]:
    result = ensure_bi_layer("metadata")
    failed = [dataset for dataset in result["datasets"] if dataset.get("status") != "ok"]
    if failed:
        raise RuntimeError(f"Failed BI datasets: {failed}")
    return result


def update_dashboard_bi_status_task() -> dict[str, Any]:
    return validate_bi_dashboards()


def pretty_json(payload: Any) -> str:
    return safe_json_dumps(payload, indent=2, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision and validate the ONOV8 Superset BI layer")
    parser.add_argument(
        "command",
        choices=[
            "refresh",
            "fix-dashboards",
            "generate-new-dashboards",
            "delete-generated-dashboards",
            "validate",
            "validate-dashboards",
            "validate-trino-views",
            "validate-superset-connection",
            "validate-datasets",
            "update-dashboard-bi-status",
            "health",
            "overview",
        ],
    )
    args = parser.parse_args()
    logger = setup_logging("bi_layer")
    commands = {
        "refresh": lambda: ensure_bi_layer("metadata"),
        "fix-dashboards": fix_superset_dashboards,
        "generate-new-dashboards": generate_new_superset_dashboards,
        "delete-generated-dashboards": delete_generated_dashboards,
        "validate": validate_bi_layer,
        "validate-dashboards": validate_bi_dashboards,
        "validate-trino-views": validate_trino_views_task,
        "validate-superset-connection": validate_superset_connection_task,
        "validate-datasets": validate_datasets_task,
        "update-dashboard-bi-status": update_dashboard_bi_status_task,
        "health": superset_health,
        "overview": bi_overview,
    }
    result = commands[args.command]()
    logger.info("%s", pretty_json(result))
    print(pretty_json(result))
    if isinstance(result, dict) and result.get("status") not in {"ok", "slow"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
