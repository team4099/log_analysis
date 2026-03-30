#!/usr/bin/env python3

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

import pandas as pd
import streamlit as st


DEFAULT_DATASETS_ROOT = Path(__file__).resolve().parent / "logs"
DEFAULT_APP_CONFIG_PATH = Path(__file__).resolve().parent / "battery_results_app_config.json"
DEFAULT_GITHUB_LOGS_BASE_URL = "https://raw.githubusercontent.com/team4099/log_analysis/main/logs"
RATING_ORDER = ["Critical", "Poor", "Fair", "Good", "Excellent", "Unknown"]
BATTERY_CONDITION_ORDER = ["Poor", "Fair", "Good", "Excellent", "Unknown"]
LOAD_ASSESSMENT_ORDER = ["Extreme", "High", "Moderate", "Normal", "Unknown"]
DOMINANT_CAUSE_ORDER = ["battery", "load", "mixed", "Unknown"]
DEFAULT_APP_CONFIG: dict[str, Any] = {
    "datasets_root": str(DEFAULT_DATASETS_ROOT),
    "default_dataset": "2026vache",
    "github_logs_base_url": DEFAULT_GITHUB_LOGS_BASE_URL,
    "comparison_metric": "p99_delta_vs_peer_median",
    "comparison_top_n": 8,
    "display_names": {},
    "subsystem_groups": [
        {"name": "Module 0", "patterns": ["Drive/Module0/drive", "Drive/Module0/turn"]},
        {"name": "Module 1", "patterns": ["Drive/Module1/drive", "Drive/Module1/turn"]},
        {"name": "Module 2", "patterns": ["Drive/Module2/drive", "Drive/Module2/turn"]},
        {"name": "Module 3", "patterns": ["Drive/Module3/drive", "Drive/Module3/turn"]},
        {"name": "Shooter", "patterns": ["/Shooter/ShooterLeaderStatorCurrent", "/Shooter/ShooterFollowerStatorCurrent"]},
        {"name": "Feed", "patterns": ["/Feeder/FeederStatorCurrentAmps", "/Hopper/hopperStatorCurrent", "/rollers/leaderStatorCurrentAmps", "/rollers/followerStatorCurrentAmps"]},
        {"name": "Intake", "patterns": ["/intake/intakeStatorCurrentAmps"]},
        {"name": "Climb", "patterns": ["/Climb/climbStatorCurrent"]}
    ]
}


def load_results(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("results JSON must be a list of log summaries")
    return [row for row in raw if isinstance(row, dict)]


def resolve_config_path(value: str, config_path: Path) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (config_path.parent / candidate).resolve()


def dataset_results_path(datasets_root: Path, dataset_name: str) -> Path:
    return datasets_root / dataset_name / "results" / "results.json"


def dataset_results_url(base_url: str, dataset_name: str) -> str:
    normalized_base = base_url.rstrip("/")
    return f"{normalized_base}/{dataset_name}/results/results.json"


def discover_datasets(datasets_root: Path) -> list[str]:
    if not datasets_root.exists():
        return []
    names = []
    for child in sorted(datasets_root.iterdir()):
        if not child.is_dir():
            continue
        if dataset_results_path(datasets_root, child.name).exists():
            names.append(child.name)
    return names


def normalize_github_url(source: str) -> str:
    parsed = urlparse(source)
    if parsed.scheme not in {"http", "https"}:
        return source

    if parsed.netloc != "github.com":
        return source

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 5 and parts[2] == "blob":
        owner, repo, _, branch = parts[:4]
        rest = "/".join(parts[4:])
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{rest}"
    return source


def load_results_from_source(source: str, fallback_path: Path | None = None) -> list[dict[str, Any]]:
    normalized_source = source.strip()
    if not normalized_source:
        if fallback_path is None:
            raise ValueError("results source is empty")
        return load_results(fallback_path)

    parsed = urlparse(normalized_source)
    if parsed.scheme in {"http", "https"}:
        url = normalize_github_url(normalized_source)
        try:
            with urlopen(url) as response:
                raw = json.load(response)
        except Exception:
            if fallback_path is None or not fallback_path.exists():
                raise
            raw = json.loads(fallback_path.read_text(encoding="utf-8"))
    else:
        raw = json.loads(Path(normalized_source).expanduser().read_text(encoding="utf-8"))

    if not isinstance(raw, list):
        raise ValueError("results JSON must be a list of log summaries")
    return [row for row in raw if isinstance(row, dict)]


def load_results_for_dataset(dataset_name: str, datasets_root: Path, github_logs_base_url: str) -> tuple[list[dict[str, Any]], str, Path]:
    fallback_path = dataset_results_path(datasets_root, dataset_name)
    source_url = dataset_results_url(github_logs_base_url, dataset_name)
    return load_results_from_source(source_url, fallback_path), source_url, fallback_path


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_app_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return DEFAULT_APP_CONFIG
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("streamlit app config root must be a JSON object")
    return deep_merge(DEFAULT_APP_CONFIG, loaded)


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


def normalize_records(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, dict[str, dict[str, dict[str, float]]]]:
    table_rows: list[dict[str, Any]] = []
    subsystem_stats: dict[str, dict[str, dict[str, float]]] = {}

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
                "internal_resistance_mohm": (
                    row["internal_resistance_ohm"] * 1000.0
                    if row.get("internal_resistance_ohm") is not None
                    else None
                ),
                "notes": " | ".join(row.get("notes", [])),
            }
        )

    df = pd.DataFrame(table_rows)
    if not df.empty:
        df["rating"] = pd.Categorical(df["rating"], categories=RATING_ORDER, ordered=True)
        df["battery_condition"] = pd.Categorical(df["battery_condition"], categories=BATTERY_CONDITION_ORDER, ordered=True)
        df["load_assessment"] = pd.Categorical(df["load_assessment"], categories=LOAD_ASSESSMENT_ORDER, ordered=True)
        df["dominant_cause"] = pd.Categorical(df["dominant_cause"], categories=DOMINANT_CAUSE_ORDER, ordered=True)
        df["_match_sort"] = df["match"].map(match_sort_key)
        df = df.sort_values(["_match_sort", "rating"], ascending=[True, True]).reset_index(drop=True)
        df = df.drop(columns=["_match_sort"])
    return df, subsystem_stats


def display_name(name: str, app_config: dict[str, Any]) -> str:
    mapped = app_config.get("display_names", {}).get(name)
    if mapped:
        return mapped
    return name.lstrip("/")


def subsystem_dataframe(subsystem_map: dict[str, dict[str, float]]) -> pd.DataFrame:
    rows = []
    for name, stats in subsystem_map.items():
        rows.append(
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
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        sort_column = "p99_a" if "p99_a" in df.columns else "peak_a"
        df = df.sort_values(sort_column, ascending=False).reset_index(drop=True)
    return df


def classify_subsystem_group(name: str, app_config: dict[str, Any]) -> str:
    for group in app_config["subsystem_groups"]:
        for pattern in group["patterns"]:
            if name == pattern:
                return group["name"]
    return name


def classify_subsystem_family(name: str) -> str:
    lowered = name.lower()
    if lowered.startswith("drive/") and "/turn" in lowered:
        return "drive_turn"
    if lowered.startswith("drive/") and "/drive" in lowered:
        return "drive_drive"
    if "shooter" in lowered:
        return "shooter"
    if "feeder" in lowered:
        return "feeder"
    if "hopper" in lowered:
        return "hopper"
    if "roller" in lowered:
        return "rollers"
    if "intake" in lowered:
        return "intake"
    if "climb" in lowered:
        return "climb"
    return "other"


def get_dataset_percentile(series: pd.Series, value: float | None) -> float | None:
    if value is None:
        return None
    clean = series.dropna().sort_values()
    if clean.empty:
        return None
    return float((clean <= value).mean() * 100.0)


def compare_subsystems_to_fleet(
    selected_match: str,
    selected_subsystems: pd.DataFrame,
    all_subsystem_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, subsystem in selected_subsystems.iterrows():
        peers = []
        for match, df in all_subsystem_frames.items():
            if match == selected_match:
                continue
            peer = df[df["subsystem"] == subsystem["subsystem"]]
            if not peer.empty:
                peers.append(peer.iloc[0])
        if not peers:
            continue

        peer_df = pd.DataFrame(peers)
        rows.append(
            {
                "subsystem": subsystem["subsystem"],
                "family": subsystem["family"],
                "current_type": subsystem["current_type"],
                "source_entry": subsystem["source_entry"],
                "peak_a": subsystem["peak_a"],
                "p90_a": subsystem["p90_a"],
                "p95_a": subsystem["p95_a"],
                "p50_a": subsystem["p50_a"],
                "p99_a": subsystem["p99_a"],
                "peak_percentile": get_dataset_percentile(peer_df["peak_a"], subsystem["peak_a"]),
                "p90_percentile": get_dataset_percentile(peer_df["p90_a"], subsystem["p90_a"]),
                "p95_percentile": get_dataset_percentile(peer_df["p95_a"], subsystem["p95_a"]),
                "p99_percentile": get_dataset_percentile(peer_df["p99_a"], subsystem["p99_a"]),
                "peak_delta_vs_peer_median": subsystem["peak_a"] - float(peer_df["peak_a"].median()),
                "p90_delta_vs_peer_median": subsystem["p90_a"] - float(peer_df["p90_a"].median()),
                "p95_delta_vs_peer_median": subsystem["p95_a"] - float(peer_df["p95_a"].median()),
                "p99_delta_vs_peer_median": subsystem["p99_a"] - float(peer_df["p99_a"].median()),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["p99_percentile", "p99_delta_vs_peer_median"], ascending=[False, False]).reset_index(drop=True)
    return df


def group_subsystem_comparison(subsystem_comparison: pd.DataFrame, app_config: dict[str, Any]) -> pd.DataFrame:
    if subsystem_comparison.empty:
        return subsystem_comparison

    grouped = subsystem_comparison.copy()
    grouped["group"] = grouped["subsystem"].map(lambda name: classify_subsystem_group(name, app_config))
    summary = (
        grouped.groupby("group", as_index=False)
        .agg(
            peak_a=("peak_a", "sum"),
            p90_a=("p90_a", "sum"),
            peak_delta_vs_peer_median=("peak_delta_vs_peer_median", "sum"),
            p90_delta_vs_peer_median=("p90_delta_vs_peer_median", "sum"),
            peak_percentile=("peak_percentile", "max"),
            p90_percentile=("p90_percentile", "max"),
        )
        .sort_values(app_config["comparison_metric"], ascending=False)
        .reset_index(drop=True)
    )
    return summary


def format_delta_amps(value: float | None) -> str | None:
    if value is None:
        return None
    rounded = round(float(value), 1)
    if abs(rounded) < 0.05:
        return None
    sign = "+" if rounded > 0 else "-"
    return f"{sign}{abs(rounded):.1f} A"


def render_signed_delta_chart(comparison_table: pd.DataFrame) -> None:
    if comparison_table.empty:
        return

    chart_df = comparison_table.set_index("subsystem_display")[["p99_delta_vs_peer_median"]]
    st.bar_chart(chart_df)


def describe_component_current_shift(
    subsystem_comparison: pd.DataFrame,
    app_config: dict[str, Any],
) -> str | None:
    if subsystem_comparison.empty:
        return None

    strongest = subsystem_comparison[
        (subsystem_comparison["p99_delta_vs_peer_median"].abs() >= 4.0)
        | (subsystem_comparison["p95_delta_vs_peer_median"].abs() >= 2.0)
    ].head(3)
    if strongest.empty:
        return None

    phrases: list[str] = []
    for _, row in strongest.iterrows():
        parts = []
        p99_delta = format_delta_amps(row["p99_delta_vs_peer_median"])
        p95_delta = format_delta_amps(row["p95_delta_vs_peer_median"])
        if p99_delta is not None:
            parts.append(f"p99 {p99_delta}")
        if p95_delta is not None:
            parts.append(f"p95 {p95_delta}")
        peak_delta = format_delta_amps(row["peak_delta_vs_peer_median"])
        if peak_delta is not None:
            parts.append(f"peak {peak_delta}")
        if parts:
            phrases.append(f"{display_name(row['subsystem'], app_config)} ({', '.join(parts)} vs fleet median)")

    if not phrases:
        return None
    return "Channels drawing more than usual: " + "; ".join(phrases) + "."


def build_flags(
    selected: pd.Series,
    all_logs: pd.DataFrame,
    subsystem_df: pd.DataFrame,
    subsystem_comparison: pd.DataFrame,
    app_config: dict[str, Any],
) -> list[str]:
    flags: list[str] = []
    min_v = selected.get("min_enabled_voltage_v")
    p05_v = selected.get("p05_enabled_voltage_v")
    peak_i = selected.get("peak_current_a")
    p90_i = selected.get("current_p90_a")
    p99_i = selected.get("current_p99_a")
    p50_i = selected.get("current_p50_a")
    resistance = selected.get("internal_resistance_mohm")
    below_9 = selected.get("time_below_9v_s")
    brownouts = selected.get("brownout_events")

    if brownouts and brownouts > 0:
        flags.append(f"Brownout behavior dominated this log: {int(brownouts)} brownout transitions.")

    if min_v is not None and min_v < 8.0:
        flags.append(f"Severe voltage dip: minimum enabled voltage reached {min_v:.2f} V.")
    elif p05_v is not None and p05_v < 10.0:
        flags.append(f"Repeated sag under load: 5th percentile enabled voltage was {p05_v:.2f} V.")

    if below_9 is not None and below_9 > 0.5:
        flags.append(f"Battery spent {below_9:.2f} s below 9 V while enabled.")

    if peak_i is not None and p99_i is not None and peak_i > max(250.0, p99_i * 1.2):
        flags.append(
            f"Short burst spike: peak current {peak_i:.1f} A is still well above enabled p99 current {p99_i:.1f} A."
        )
    elif peak_i is not None and p90_i is not None and peak_i > max(250.0, p90_i * 2.0):
        flags.append(
            f"Large transient load spike: peak current {peak_i:.1f} A versus p90 current {p90_i:.1f} A."
        )
    elif peak_i is not None and p50_i is not None and peak_i > max(250.0, p50_i * 3.0):
        flags.append(
            f"Load profile is spiky: peak current {peak_i:.1f} A versus p50 current {p50_i:.1f} A."
        )

    resistance_pct = get_dataset_percentile(all_logs["internal_resistance_mohm"], resistance)
    if resistance is not None and resistance_pct is not None:
        if resistance_pct >= 85:
            flags.append(
                f"Estimated internal resistance is high relative to this dataset: {resistance:.1f} mOhm "
                f"(worse than about {100 - resistance_pct:.0f}% of logs)."
            )
        elif resistance_pct <= 30 and min_v is not None and min_v < 8.5:
            flags.append(
                f"Battery resistance looks acceptable at {resistance:.1f} mOhm, so the weak voltage "
                "was likely driven more by match load than by battery health alone."
            )

    component_shift = describe_component_current_shift(subsystem_comparison, app_config)
    if component_shift is not None:
        flags.append(component_shift)

    if not subsystem_df.empty:
        top_peaks = subsystem_df.head(3)
        top_names = ", ".join(display_name(name, app_config) for name in top_peaks["subsystem"].tolist())
        flags.append(f"Top instantaneous contributors were: {top_names}.")

        drive_turn_share = (subsystem_df[subsystem_df["family"] == "drive_turn"]["peak_a"].sum() /
                            max(subsystem_df["peak_a"].sum(), 1e-9))
        drive_drive_share = (subsystem_df[subsystem_df["family"] == "drive_drive"]["peak_a"].sum() /
                             max(subsystem_df["peak_a"].sum(), 1e-9))
        if drive_turn_share > 0.35:
            flags.append("Turn motors were a major source of current spikes in this match.")
        if drive_turn_share + drive_drive_share > 0.6:
            flags.append("Most high-load behavior came from the drivetrain rather than auxiliaries.")

    if not flags:
        flags.append("No single dominant failure signal stood out; this log looks broadly healthy.")

    return flags


def overview_column_config() -> dict[str, Any]:
    return {
        "match": st.column_config.TextColumn("Match", help="Match label parsed from the log filename."),
        "rating": st.column_config.TextColumn("Severity", help="Overall severity of the observed voltage behavior. This is not the same thing as battery quality."),
        "battery_condition": st.column_config.TextColumn("Battery", help="Estimated battery condition based mainly on internal resistance or resting voltage."),
        "load_assessment": st.column_config.TextColumn("Load", help="How hard the robot was pulling current in this match."),
        "dominant_cause": st.column_config.TextColumn("Cause", help="Best-effort attribution for whether the issue looks battery-driven, load-driven, or mixed."),
        "summary": st.column_config.TextColumn("Summary", help="Primary severity reason."),
        "min_enabled_voltage_v": st.column_config.NumberColumn("Min V", help="Minimum battery voltage while enabled.", format="%.2f V"),
        "p05_enabled_voltage_v": st.column_config.NumberColumn("P05 V", help="5th percentile battery voltage while enabled.", format="%.2f V"),
        "peak_current_a": st.column_config.NumberColumn("Peak Pack I", help="Peak estimated pack-side supply current while enabled. This is the whole-robot current model used for battery-health analysis.", format="%.1f A"),
        "current_p95_a": st.column_config.NumberColumn("P95 Pack I", help="95th percentile estimated pack-side supply current while enabled.", format="%.1f A"),
        "current_p99_a": st.column_config.NumberColumn("P99 Pack I", help="99th percentile estimated pack-side supply current while enabled.", format="%.1f A"),
        "current_p90_a": st.column_config.NumberColumn("P90 Pack I", help="90th percentile estimated pack-side supply current while enabled.", format="%.1f A"),
        "internal_resistance_mohm": st.column_config.NumberColumn("Rint", help="Estimated effective internal resistance.", format="%.1f mOhm"),
        "brownout_events": st.column_config.NumberColumn("Brownouts", help="Number of browned-out transitions.")
    }


def subsystem_column_config() -> dict[str, Any]:
    return {
        "subsystem_display": st.column_config.TextColumn("Channel", help="Motor or mechanism channel name used for subsystem inspection."),
        "current_type": st.column_config.TextColumn("Current Basis", help="Whether this subsystem row is using stator current or supply current. Stator current reflects motor-side load; supply current reflects battery-side controller input."),
        "source_entry": st.column_config.TextColumn("Telemetry Entry", help="Raw AdvantageKit/WPILOG entry used for this subsystem row."),
        "family": st.column_config.TextColumn("Family", help="Heuristic channel family."),
        "p50_a": st.column_config.NumberColumn("P50 Channel I", help="50th percentile enabled current for this specific channel, using the basis shown in Current Basis.", format="%.2f A"),
        "p90_a": st.column_config.NumberColumn("P90 Channel I", help="90th percentile enabled current for this specific channel.", format="%.2f A"),
        "p95_a": st.column_config.NumberColumn("P95 Channel I", help="95th percentile enabled current for this specific channel.", format="%.2f A"),
        "p99_a": st.column_config.NumberColumn("P99 Channel I", help="99th percentile enabled current for this specific channel.", format="%.2f A"),
        "peak_a": st.column_config.NumberColumn("Peak Channel I", help="Peak enabled current for this specific channel.", format="%.2f A"),
    }


def comparison_column_config() -> dict[str, Any]:
    return {
        "subsystem_display": st.column_config.TextColumn("Channel", help="Motor or channel being compared to the same channel across other matches."),
        "current_type": st.column_config.TextColumn("Current Basis", help="Whether this comparison is using stator current or supply current for that channel."),
        "source_entry": st.column_config.TextColumn("Telemetry Entry", help="Raw telemetry channel behind this comparison row."),
        "peak_a": st.column_config.NumberColumn("Peak Channel I", help="Peak enabled current for this channel in the selected match.", format="%.2f A"),
        "p90_a": st.column_config.NumberColumn("P90 Channel I", help="90th percentile enabled current for this channel in the selected match.", format="%.2f A"),
        "p95_a": st.column_config.NumberColumn("P95 Channel I", help="95th percentile enabled current for this channel in the selected match.", format="%.2f A"),
        "p99_a": st.column_config.NumberColumn("P99 Channel I", help="99th percentile enabled current for this channel in the selected match.", format="%.2f A"),
        "p99_percentile": st.column_config.NumberColumn("P99 %ile", help="Percentile of this channel's p99 current versus the same channel in other matches.", format="%.0f"),
        "p95_percentile": st.column_config.NumberColumn("P95 %ile", help="Percentile of this channel's p95 current versus the same channel in other matches.", format="%.0f"),
        "p99_delta_vs_peer_median": st.column_config.NumberColumn("P99 Δ", help="Difference from the fleet median p99 for this same channel and same current basis.", format="%.2f A"),
        "p95_delta_vs_peer_median": st.column_config.NumberColumn("P95 Δ", help="Difference from the fleet median p95 for this same channel and same current basis.", format="%.2f A"),
        "p90_delta_vs_peer_median": st.column_config.NumberColumn("P90 Δ", help="Difference from the fleet median p90 for this same channel and same current basis.", format="%.2f A"),
    }


def metric_delta_text(value: float | None, benchmark: float | None, inverse: bool = False) -> str | None:
    if value is None or benchmark is None:
        return None
    delta = value - benchmark
    if abs(delta) < 1e-9:
        return "at fleet median"
    direction = "below" if delta < 0 else "above"
    if inverse:
        direction = "better than" if delta < 0 else "worse than"
    return f"{abs(delta):.2f} {direction} median"


def render_overview(df: pd.DataFrame) -> None:
    st.subheader("Fleet Overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Logs", len(df))
    c2.metric("Critical", int((df["rating"] == "Critical").sum()))
    c3.metric("Poor Batteries", int((df["battery_condition"] == "Poor").sum()))
    c4.metric("Load-Driven Criticals", int(((df["dominant_cause"] == "load") & (df["rating"] == "Critical")).sum()))

    severity_counts = (
        df["rating"].astype(str).value_counts().reindex(RATING_ORDER, fill_value=0).rename_axis("severity").reset_index(name="count")
    )
    battery_counts = (
        df["battery_condition"].astype(str).value_counts().reindex(BATTERY_CONDITION_ORDER, fill_value=0).rename_axis("battery_condition").reset_index(name="count")
    )
    cause_counts = (
        df["dominant_cause"].astype(str).value_counts().reindex(DOMINANT_CAUSE_ORDER, fill_value=0).rename_axis("dominant_cause").reset_index(name="count")
    )

    st.markdown("**Severity vs Battery Condition vs Cause**")
    a, b = st.columns(2)
    a.caption("Overall severity")
    a.bar_chart(severity_counts.set_index("severity"))
    b.caption("Battery condition")
    b.bar_chart(battery_counts.set_index("battery_condition"))
    st.caption("Dominant cause")
    st.bar_chart(cause_counts.set_index("dominant_cause"))

    st.dataframe(
        df[
            [
                "match",
                "rating",
                "battery_condition",
                "dominant_cause",
                "summary",
                "min_enabled_voltage_v",
                "p05_enabled_voltage_v",
                "peak_current_a",
                "current_p95_a",
                "current_p99_a",
                "current_p90_a",
                "internal_resistance_mohm",
                "brownout_events",
            ]
        ],
        use_container_width=True,
        hide_index=True,
        column_config=overview_column_config(),
    )


def render_match_page(
    df: pd.DataFrame,
    subsystem_stats: dict[str, dict[str, dict[str, float]]],
    all_subsystem_frames: dict[str, pd.DataFrame],
    app_config: dict[str, Any],
) -> None:
    st.subheader("Match Detail")
    selected_match = st.selectbox("Match", df["match"].tolist(), index=0)
    selected = df[df["match"] == selected_match].iloc[0]
    render_selected_log(selected, df, subsystem_stats.get(selected["log_path"], {}), all_subsystem_frames, app_config)


def render_selected_log(
    selected: pd.Series,
    df: pd.DataFrame,
    subsystem_map: dict[str, dict[str, float]],
    all_subsystem_frames: dict[str, pd.DataFrame],
    app_config: dict[str, Any],
) -> None:
    subsystem_df = subsystem_dataframe(subsystem_map)
    subsystem_comparison = compare_subsystems_to_fleet(selected["match"], subsystem_df, all_subsystem_frames)
    flags = build_flags(selected, df, subsystem_df, subsystem_comparison, app_config)

    st.subheader(f"Selected Log: {selected['match']}")
    st.caption(selected["log_path"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Severity", str(selected["rating"]))
    c2.metric(
        "Battery Condition",
        str(selected["battery_condition"]),
        None,
    )
    c3.metric(
        "Dominant Cause",
        str(selected["dominant_cause"]),
        None,
    )
    c4.metric(
        "Brownouts",
        f"{int(selected['brownout_events'])}" if pd.notna(selected["brownout_events"]) else "n/a",
        None,
    )

    e1, e2, e3 = st.columns(3)
    e1.metric(
        "Min Voltage",
        f"{selected['min_enabled_voltage_v']:.2f} V" if pd.notna(selected["min_enabled_voltage_v"]) else "n/a",
        metric_delta_text(selected["min_enabled_voltage_v"], df["min_enabled_voltage_v"].median()),
    )
    e2.metric(
        "P99 Pack Current",
        f"{selected['current_p99_a']:.1f} A" if pd.notna(selected["current_p99_a"]) else "n/a",
        metric_delta_text(selected["current_p99_a"], df["current_p99_a"].median()),
    )
    e3.metric(
        "Resistance",
        f"{selected['internal_resistance_mohm']:.1f} mOhm" if pd.notna(selected["internal_resistance_mohm"]) else "n/a",
        metric_delta_text(selected["internal_resistance_mohm"], df["internal_resistance_mohm"].median(), inverse=True),
    )

    st.markdown("**Interpretation**")
    st.write(f"- Battery condition: {selected.get('battery_condition_summary') or 'n/a'}")
    st.write(f"- Load assessment: {selected.get('load_assessment_summary') or 'n/a'}")

    st.markdown("**What Went Wrong**")
    for flag in flags:
        st.write(f"- {flag}")

    st.markdown("**Whole-Robot Estimated Pack Current While Enabled**")
    st.caption("This is estimated pack-side supply current, meaning the battery-side current drawn by the robot as a whole. It is not motor stator current.")
    current_stats = [
        ("p50 current", selected.get("current_p50_a")),
        ("p90 current", selected.get("current_p90_a")),
        ("p95 current", selected.get("current_p95_a")),
        ("p99 current", selected.get("current_p99_a")),
        ("peak current", selected.get("peak_current_a")),
    ]
    current_df = pd.DataFrame(
        [{"metric": label, "amps": value} for label, value in current_stats if value is not None]
    )
    if not current_df.empty:
        st.bar_chart(current_df.set_index("metric"))

    st.markdown("**Per-Channel Enabled Current Breakdown**")
    if subsystem_df.empty:
        st.info("No subsystem current breakdown was present in this results file.")
    else:
        subsystem_table = subsystem_df.copy()
        subsystem_table["subsystem_display"] = subsystem_table["subsystem"].map(lambda name: display_name(name, app_config))
        st.dataframe(
            subsystem_table.head(12)[["subsystem_display", "current_type", "source_entry", "family", "p50_a", "p90_a", "p95_a", "p99_a", "peak_a"]],
            use_container_width=True,
            hide_index=True,
            column_config=subsystem_column_config(),
        )
        p99_view = subsystem_table.head(12).set_index("subsystem_display")[["p99_a"]]
        st.bar_chart(p99_view)

        st.markdown("**Channels Higher Than Usual In This Match (Same Channel, Same Current Basis)**")
        st.caption("Positive values mean this channel drew more current than its fleet median. Negative values mean it drew less.")
        if subsystem_comparison.empty:
            st.info("No fleet comparison was available for subsystem currents.")
        else:
            comparison_table = subsystem_comparison.copy()
            comparison_table["subsystem_display"] = comparison_table["subsystem"].map(lambda name: display_name(name, app_config))
            render_signed_delta_chart(comparison_table)
            st.dataframe(
                comparison_table[
                    [
                        "subsystem_display",
                        "current_type",
                        "source_entry",
                        "p90_a",
                        "p95_a",
                        "p99_a",
                        "p95_percentile",
                        "p99_percentile",
                        "p95_delta_vs_peer_median",
                        "p99_delta_vs_peer_median",
                        "p90_delta_vs_peer_median",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config=comparison_column_config(),
            )


def main() -> None:
    st.set_page_config(page_title="Battery Log Analysis", layout="wide")
    st.title("Battery Log Analysis")
    st.caption("Inspect analyzed match logs, compare fleet behavior, and understand why a match looked weak.")

    try:
        app_config = load_app_config(DEFAULT_APP_CONFIG_PATH)
    except Exception as exc:
        st.error(f"Failed to load app config: {exc}")
        st.stop()

    datasets_root = resolve_config_path(app_config["datasets_root"], DEFAULT_APP_CONFIG_PATH)
    dataset_names = discover_datasets(datasets_root)
    default_dataset = app_config.get("default_dataset")
    if default_dataset not in dataset_names and dataset_names:
        default_dataset = dataset_names[0]

    with st.sidebar:
        st.header("Results Source")
        source_mode = st.radio("Source", ["Example Dataset", "Custom Path or URL", "Upload"], index=0)

        selected_dataset = None
        path_text = ""
        uploaded = None
        source_label = ""

        if source_mode == "Example Dataset":
            if not dataset_names:
                st.warning(f"No datasets found under {datasets_root}.")
            else:
                default_index = dataset_names.index(default_dataset) if default_dataset in dataset_names else 0
                selected_dataset = st.selectbox("Example Name", dataset_names, index=default_index)
                remote_url = dataset_results_url(app_config["github_logs_base_url"], selected_dataset)
                local_path = dataset_results_path(datasets_root, selected_dataset)
                source_label = f"Dataset `{selected_dataset}`"
                st.caption(f"GitHub source: {remote_url}")
                if local_path.exists():
                    st.caption(f"Local fallback: {local_path}")
        elif source_mode == "Custom Path or URL":
            path_text = st.text_input("Results JSON path or URL", "")
        else:
            uploaded = st.file_uploader("Upload a results.json", type=["json"])

    try:
        if source_mode == "Upload":
            if uploaded is None:
                st.info("Upload a `results.json` file to inspect it.")
                st.stop()
            records = json.load(uploaded)
            source_label = "Uploaded results.json"
        elif source_mode == "Custom Path or URL":
            if not path_text.strip():
                st.info("Enter a local path or URL for a `results.json` file.")
                st.stop()
            records = load_results_from_source(path_text)
            source_label = path_text
        else:
            if selected_dataset is None:
                st.stop()
            records, _, _ = load_results_for_dataset(
                selected_dataset,
                datasets_root,
                app_config["github_logs_base_url"],
            )
    except Exception as exc:
        st.error(f"Failed to load results: {exc}")
        st.stop()

    st.caption(f"Loaded source: {source_label}")

    df, subsystem_stats = normalize_records(records)
    if df.empty:
        st.warning("No log summaries were found in this JSON file.")
        st.stop()

    all_subsystem_frames = {
        row["match"]: subsystem_dataframe(subsystem_stats.get(row["log_path"], {}))
        for _, row in df.iterrows()
    }

    with st.sidebar:
        st.header("View")
        page = st.radio("Page", ["Fleet Overview", "Match Detail"], index=0, label_visibility="collapsed")

    if page == "Fleet Overview":
        render_overview(df)
    else:
        render_match_page(df, subsystem_stats, all_subsystem_frames, app_config)


if __name__ == "__main__":
    main()
