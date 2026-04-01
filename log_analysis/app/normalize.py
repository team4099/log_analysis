from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from .comparison import classify_subsystem_family


RATING_ORDER = ["Critical", "Poor", "Fair", "Good", "Excellent", "Unknown"]
BATTERY_CONDITION_ORDER = ["Poor", "Fair", "Good", "Excellent", "Unknown"]
LOAD_ASSESSMENT_ORDER = ["Extreme", "High", "Moderate", "Normal", "Unknown"]
DOMINANT_CAUSE_ORDER = ["battery", "load", "mixed", "Unknown"]
PHASE_OPTIONS = {
    "Auto + Teleop": "all_enabled",
    "Auto Only": "auto",
    "Teleop Only": "teleop",
}


def get_match_label(log_path: str) -> str:
    name = Path(log_path).stem
    parts = name.split("_")
    return parts[-1] if parts else name


def match_sort_key(match_label: str) -> tuple[int, int, str]:
    lowered = match_label.lower()
    match = re.fullmatch(r"([a-z]+)(\d+)", lowered)
    if match is None:
        return (2, 10**9, lowered)

    prefix, number = match.groups()
    if prefix == "q":
        return (0, int(number), lowered)
    if prefix == "e":
        return (1, int(number), lowered)
    return (2, int(number), lowered)


def apply_table_categories(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["rating"] = pd.Categorical(df["rating"], categories=RATING_ORDER, ordered=True)
    df["battery_condition"] = pd.Categorical(df["battery_condition"], categories=BATTERY_CONDITION_ORDER, ordered=True)
    df["load_assessment"] = pd.Categorical(df["load_assessment"], categories=LOAD_ASSESSMENT_ORDER, ordered=True)
    df["dominant_cause"] = pd.Categorical(df["dominant_cause"], categories=DOMINANT_CAUSE_ORDER, ordered=True)
    df["_match_sort"] = df["match"].map(match_sort_key)
    df = df.sort_values(["_match_sort", "rating"], ascending=[True, True]).reset_index(drop=True)
    return df.drop(columns=["_match_sort"])


def summary_to_row(path: str, match_label: str, summary: dict[str, Any], base_row: dict[str, Any]) -> dict[str, Any]:
    current_stats = summary.get("current_stats") or {}
    return {
        **base_row,
        "match": match_label,
        "log_path": path,
        "rating": summary.get("rating", base_row.get("rating", "Unknown")),
        "summary": summary.get("summary", base_row.get("summary", "")),
        "battery_condition": summary.get("battery_condition", base_row.get("battery_condition", "Unknown")) or "Unknown",
        "battery_condition_summary": summary.get("battery_condition_summary", base_row.get("battery_condition_summary", "")),
        "load_assessment": summary.get("load_assessment", base_row.get("load_assessment", "Unknown")) or "Unknown",
        "load_assessment_summary": summary.get("load_assessment_summary", base_row.get("load_assessment_summary", "")),
        "dominant_cause": summary.get("dominant_cause", base_row.get("dominant_cause", "Unknown")) or "Unknown",
        "enabled_duration_s": summary.get("enabled_duration_s"),
        "resting_voltage_v": base_row.get("resting_voltage_v"),
        "min_enabled_voltage_v": summary.get("min_enabled_voltage_v"),
        "p05_enabled_voltage_v": summary.get("p05_enabled_voltage_v"),
        "brownout_events": summary.get("brownout_events"),
        "time_below_9v_s": summary.get("time_below_9v_s"),
        "time_below_10v_s": summary.get("time_below_10v_s"),
        "peak_current_a": summary.get("peak_current_a"),
        "current_p50_a": current_stats.get("p50_a"),
        "current_p90_a": current_stats.get("p90_a"),
        "current_p95_a": current_stats.get("p95_a"),
        "current_p99_a": current_stats.get("p99_a"),
        "internal_resistance_mohm": (
            summary["internal_resistance_ohm"] * 1000.0
            if summary.get("internal_resistance_ohm") is not None
            else None
        ),
        "notes": base_row.get("notes", ""),
    }


def normalize_records(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, dict[str, dict[str, dict[str, float]]], dict[str, dict[str, dict[str, Any]]]]:
    table_rows: list[dict[str, Any]] = []
    subsystem_stats: dict[str, dict[str, dict[str, float]]] = {}
    phase_summaries_by_log: dict[str, dict[str, dict[str, Any]]] = {}

    for row in records:
        path = row.get("log_path", "")
        match_label = get_match_label(path)
        current_stats = row.get("current_stats") or {}
        subsystem_map = row.get("subsystem_current_stats") or {}
        subsystem_stats[path] = subsystem_map
        table_rows.append(
            {
                "match": match_label,
                "log_path": path,
                "rating": row.get("rating", "Unknown"),
                "summary": row.get("summary", ""),
                "battery_condition": row.get("battery_condition", "Unknown") or "Unknown",
                "battery_condition_summary": row.get("battery_condition_summary", ""),
                "load_assessment": row.get("load_assessment", "Unknown") or "Unknown",
                "load_assessment_summary": row.get("load_assessment_summary", ""),
                "dominant_cause": row.get("dominant_cause", "Unknown") or "Unknown",
                "enabled_duration_s": row.get("enabled_duration_s"),
                "resting_voltage_v": row.get("resting_voltage_v"),
                "min_enabled_voltage_v": row.get("min_enabled_voltage_v"),
                "p05_enabled_voltage_v": row.get("p05_enabled_voltage_v"),
                "brownout_events": row.get("brownout_events"),
                "time_below_9v_s": row.get("time_below_9v_s"),
                "time_below_10v_s": row.get("time_below_10v_s"),
                "peak_current_a": row.get("peak_current_a"),
                "current_p50_a": current_stats.get("p50_a"),
                "current_p90_a": current_stats.get("p90_a"),
                "current_p95_a": current_stats.get("p95_a"),
                "current_p99_a": current_stats.get("p99_a"),
                "internal_resistance_mohm": row["internal_resistance_ohm"] * 1000.0 if row.get("internal_resistance_ohm") is not None else None,
                "notes": " | ".join(row.get("notes", [])),
            }
        )
        phase_summaries_by_log[path] = row.get("phase_summaries") or {}

    return apply_table_categories(pd.DataFrame(table_rows)), subsystem_stats, phase_summaries_by_log


def extract_traces_by_log(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    traces_by_log: dict[str, dict[str, Any]] = {}
    for row in records:
        log_path = row.get("log_path")
        trace = row.get("trace")
        if isinstance(log_path, str) and isinstance(trace, dict):
            traces_by_log[log_path] = trace
    return traces_by_log


def trace_dataframe(trace: dict[str, Any] | None) -> pd.DataFrame:
    if not trace:
        return pd.DataFrame()
    rows = []
    for point in trace.get("points", []):
        if not isinstance(point, dict):
            continue
        rows.append(
            {
                "time_s": point.get("time_s"),
                "voltage_v": point.get("voltage_v"),
                "pack_current_a": point.get("pack_current_a"),
                "enabled": int(bool(point.get("enabled"))),
                "autonomous": int(bool(point.get("autonomous"))),
                "browned_out": int(bool(point.get("browned_out"))),
            }
        )
    return pd.DataFrame(rows)


def display_name(name: str, app_config: dict[str, Any]) -> str:
    return app_config.get("display_names", {}).get(name) or name.lstrip("/")


def build_phase_dataframe(
    df: pd.DataFrame,
    phase_summaries_by_log: dict[str, dict[str, dict[str, Any]]],
    phase_key: str,
) -> pd.DataFrame:
    if phase_key == "all_enabled":
        return df

    phase_rows = []
    for row in df.to_dict("records"):
        phase_summary = phase_summaries_by_log.get(row["log_path"], {}).get(phase_key)
        phase_rows.append(summary_to_row(row["log_path"], row["match"], phase_summary, row) if phase_summary else dict(row))
    return apply_table_categories(pd.DataFrame(phase_rows))


def subsystem_map_for_phase(
    log_path: str,
    subsystem_stats: dict[str, dict[str, dict[str, float]]],
    phase_summaries_by_log: dict[str, dict[str, dict[str, Any]]],
    phase_key: str,
) -> dict[str, dict[str, float]]:
    if phase_key == "all_enabled":
        return subsystem_stats.get(log_path, {})
    phase_summary = phase_summaries_by_log.get(log_path, {}).get(phase_key) or {}
    subsystem_map = phase_summary.get("subsystem_current_stats")
    return subsystem_stats.get(log_path, {}) if subsystem_map is None else subsystem_map


def build_subsystem_frames_for_phase(
    phase_df: pd.DataFrame,
    subsystem_stats: dict[str, dict[str, dict[str, float]]],
    phase_summaries_by_log: dict[str, dict[str, dict[str, Any]]],
    phase_key: str,
) -> dict[str, pd.DataFrame]:
    return {
        row["match"]: subsystem_dataframe(
            subsystem_map_for_phase(row["log_path"], subsystem_stats, phase_summaries_by_log, phase_key)
        )
        for _, row in phase_df.iterrows()
    }


def has_phase_data(phase_summaries_by_log: dict[str, dict[str, dict[str, Any]]], phase_key: str) -> bool:
    if phase_key == "all_enabled":
        return True
    return any(phase_key in summaries for summaries in phase_summaries_by_log.values())


def subsystem_dataframe(subsystem_map: dict[str, dict[str, float]]) -> pd.DataFrame:
    rows = [
        {
            "subsystem": name,
            "current_type": stats.get("current_type", "unknown"),
            "source_entry": stats.get("source_entry", name),
            "p50_a": stats.get("p50_a"),
            "p90_a": stats.get("p90_a"),
            "p95_a": stats.get("p95_a"),
            "p99_a": stats.get("p99_a"),
            "peak_a": stats.get("peak_a"),
            "family": classify_subsystem_family(name),
        }
        for name, stats in subsystem_map.items()
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        sort_column = "p99_a" if "p99_a" in df.columns else "peak_a"
        df = df.sort_values(sort_column, ascending=False).reset_index(drop=True)
    return df
