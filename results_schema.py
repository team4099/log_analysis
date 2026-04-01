from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


RESULTS_SCHEMA_VERSION = "1.0"
ANALYZER_NAME = "battery_health.py"


def _normalize_record_list(raw_records: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_records, list):
        raise ValueError("results JSON records must be a list")
    return [row for row in raw_records if isinstance(row, dict)]


def parse_results_payload(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return _normalize_record_list(raw)
    if isinstance(raw, dict):
        return _normalize_record_list(raw.get("records"))
    raise ValueError("results JSON must be a list of log summaries or an object containing a records list")


def infer_dataset_name(log_paths: Iterable[Path], output_path: Path | None = None) -> str | None:
    if output_path is not None:
        resolved_output = output_path.expanduser().resolve()
        if (
            resolved_output.name == "results.json"
            and resolved_output.parent.name == "results"
            and resolved_output.parent.parent.name
        ):
            return resolved_output.parent.parent.name

    resolved_logs = [path.expanduser().resolve() for path in log_paths]
    if not resolved_logs:
        return None

    parent_names = {path.parent.name for path in resolved_logs}
    if len(parent_names) == 1:
        return next(iter(parent_names))
    return None


def build_results_payload(
    records: list[dict[str, Any]],
    log_paths: Iterable[Path],
    output_path: Path | None = None,
    analyzer_name: str = ANALYZER_NAME,
    analyzer_version: str = RESULTS_SCHEMA_VERSION,
) -> dict[str, Any]:
    return {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "dataset_metadata": {
            "dataset_name": infer_dataset_name(log_paths, output_path),
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "generator": analyzer_name,
            "analyzer_version": analyzer_version,
            "record_count": len(records),
        },
        "records": records,
    }


def dump_results_payload(
    records: list[dict[str, Any]],
    log_paths: Iterable[Path],
    output_path: Path | None = None,
) -> str:
    return json.dumps(build_results_payload(records, log_paths, output_path), indent=2)
