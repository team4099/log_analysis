from __future__ import annotations

from typing import Any

import pandas as pd


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
    if selected_subsystems.empty or "subsystem" not in selected_subsystems.columns:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for _, subsystem in selected_subsystems.iterrows():
        peers = []
        for match, df in all_subsystem_frames.items():
            if match == selected_match or df.empty or "subsystem" not in df.columns:
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
    return (
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


def _median_or_none(series: pd.Series) -> float | None:
    clean = series.dropna()
    if clean.empty:
        return None
    return float(clean.median())


def summarize_dataset(df: pd.DataFrame) -> dict[str, float | int | None]:
    if df.empty:
        return {
            "log_count": 0,
            "critical_count": 0,
            "poor_battery_count": 0,
            "brownout_log_count": 0,
            "median_min_enabled_voltage_v": None,
            "median_p99_pack_current_a": None,
            "median_internal_resistance_mohm": None,
            "median_enabled_duration_s": None,
        }

    brownout_series = df["brownout_events"] if "brownout_events" in df.columns else pd.Series(dtype=float)
    return {
        "log_count": int(len(df)),
        "critical_count": int((df["rating"] == "Critical").sum()),
        "poor_battery_count": int((df["battery_condition"] == "Poor").sum()),
        "brownout_log_count": int((brownout_series.fillna(0) > 0).sum()),
        "median_min_enabled_voltage_v": _median_or_none(df["min_enabled_voltage_v"]),
        "median_p99_pack_current_a": _median_or_none(df["current_p99_a"]),
        "median_internal_resistance_mohm": _median_or_none(df["internal_resistance_mohm"]),
        "median_enabled_duration_s": _median_or_none(df["enabled_duration_s"]),
    }


def build_dataset_comparison_table(
    primary_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    primary_label: str,
    comparison_label: str,
) -> pd.DataFrame:
    primary = summarize_dataset(primary_df)
    comparison = summarize_dataset(comparison_df)
    rows = [
        ("Logs", "log_count", ""),
        ("Critical Logs", "critical_count", ""),
        ("Poor Batteries", "poor_battery_count", ""),
        ("Logs With Brownouts", "brownout_log_count", ""),
        ("Median Min Voltage", "median_min_enabled_voltage_v", "V"),
        ("Median P99 Pack Current", "median_p99_pack_current_a", "A"),
        ("Median Effective Resistance", "median_internal_resistance_mohm", "mOhm"),
        ("Median Enabled Duration", "median_enabled_duration_s", "s"),
    ]

    table_rows: list[dict[str, Any]] = []
    for label, key, unit in rows:
        primary_value = primary.get(key)
        comparison_value = comparison.get(key)
        delta = None
        if isinstance(primary_value, (int, float)) and isinstance(comparison_value, (int, float)):
            delta = float(primary_value - comparison_value)
        table_rows.append(
            {
                "metric": label,
                primary_label: primary_value,
                comparison_label: comparison_value,
                "delta": delta,
                "unit": unit,
            }
        )

    return pd.DataFrame(table_rows)
