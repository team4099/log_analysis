from __future__ import annotations

from typing import Any

import pandas as pd

from .comparison import get_dataset_percentile
from .normalize import display_name


def format_delta_amps(value: float | None) -> str | None:
    if value is None:
        return None
    rounded = round(float(value), 1)
    if abs(rounded) < 0.05:
        return None
    sign = "+" if rounded > 0 else "-"
    return f"{sign}{abs(rounded):.1f} A"


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

    return None if not phrases else "Channels drawing more than usual: " + "; ".join(phrases) + "."


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
        flags.append(f"Short burst spike: peak current {peak_i:.1f} A is still well above enabled p99 current {p99_i:.1f} A.")
    elif peak_i is not None and p90_i is not None and peak_i > max(250.0, p90_i * 2.0):
        flags.append(f"Large transient load spike: peak current {peak_i:.1f} A versus p90 current {p90_i:.1f} A.")
    elif peak_i is not None and p50_i is not None and peak_i > max(250.0, p50_i * 3.0):
        flags.append(f"Load profile is spiky: peak current {peak_i:.1f} A versus p50 current {p50_i:.1f} A.")

    resistance_pct = get_dataset_percentile(all_logs["internal_resistance_mohm"], resistance)
    if resistance is not None and resistance_pct is not None:
        if resistance_pct >= 85:
            flags.append(
                f"Estimated effective resistance in this selected phase is high relative to this dataset: {resistance:.1f} mOhm "
                f"(worse than about {100 - resistance_pct:.0f}% of logs)."
            )
        elif resistance_pct <= 30 and min_v is not None and min_v < 8.5:
            flags.append(
                f"Effective resistance in this selected phase looks acceptable at {resistance:.1f} mOhm, so the weak voltage "
                "was likely driven more by phase load than by battery health alone."
            )

    component_shift = describe_component_current_shift(subsystem_comparison, app_config)
    if component_shift is not None:
        flags.append(component_shift)

    if not subsystem_df.empty:
        top_peaks = subsystem_df.head(3)
        top_names = ", ".join(display_name(name, app_config) for name in top_peaks["subsystem"].tolist())
        flags.append(f"Top instantaneous contributors were: {top_names}.")

        total_peak = max(subsystem_df["peak_a"].sum(), 1e-9)
        drive_turn_share = subsystem_df[subsystem_df["family"] == "drive_turn"]["peak_a"].sum() / total_peak
        drive_drive_share = subsystem_df[subsystem_df["family"] == "drive_drive"]["peak_a"].sum() / total_peak
        if drive_turn_share > 0.35:
            flags.append("Turn motors were a major source of current spikes in this match.")
        if drive_turn_share + drive_drive_share > 0.6:
            flags.append("Most high-load behavior came from the drivetrain rather than auxiliaries.")

    if not flags:
        flags.append("No single dominant failure signal stood out; this log looks broadly healthy.")
    return flags
