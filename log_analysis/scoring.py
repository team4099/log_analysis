from __future__ import annotations

from typing import Any

def rate_battery(
    min_enabled_voltage_v: float | None,
    p05_enabled_voltage_v: float | None,
    brownout_events: int,
    time_below_brownout_s: float,
    internal_resistance_ohm: float | None,
    config: dict[str, Any],
) -> tuple[str, str]:
    if min_enabled_voltage_v is None or p05_enabled_voltage_v is None:
        return "Unknown", "Not enough enabled battery samples to score this log."

    thresholds = config["thresholds"]
    if (
        brownout_events > 0
        or time_below_brownout_s > thresholds["critical_time_below_brownout_s"]
        or min_enabled_voltage_v < thresholds["critical_min_voltage_v"]
    ):
        return "Critical", "Battery hit or crossed the brownout region under load."

    if internal_resistance_ohm is not None:
        resistance_mohm = internal_resistance_ohm * 1000.0
        r = thresholds["resistance"]
        if resistance_mohm > r["fair_max_mohm"]:
            return "Poor", "Estimated effective battery resistance is high for the observed load."
        if resistance_mohm > r["good_max_mohm"]:
            return "Fair", "Estimated effective battery resistance is usable but not strong."
        if resistance_mohm > r["excellent_max_mohm"]:
            return "Good", "Estimated effective battery resistance looks healthy."
        return "Excellent", "Estimated effective battery resistance looks very strong."

    fallback = thresholds["fallback_voltage"]
    if p05_enabled_voltage_v < fallback["poor"]["p05_v"] or min_enabled_voltage_v < fallback["poor"]["min_v"]:
        return "Poor", "Battery sagged heavily during enabled operation."
    if p05_enabled_voltage_v < fallback["fair"]["p05_v"] or min_enabled_voltage_v < fallback["fair"]["min_v"]:
        return "Fair", "Battery stayed usable but dipped lower than ideal for match load."
    if p05_enabled_voltage_v < fallback["good"]["p05_v"] or min_enabled_voltage_v < fallback["good"]["min_v"]:
        return "Good", "Battery sag looks acceptable with some headroom loss."
    return "Excellent", "Battery held voltage well throughout enabled operation."


def rate_battery_condition(
    internal_resistance_ohm: float | None,
    resting_voltage_v: float | None,
    config: dict[str, Any],
) -> tuple[str | None, str | None]:
    if internal_resistance_ohm is not None:
        resistance_mohm = internal_resistance_ohm * 1000.0
        r = config["thresholds"]["resistance"]
        if resistance_mohm > r["fair_max_mohm"]:
            return "Poor", "Internal resistance is high, which points to a weak battery."
        if resistance_mohm > r["good_max_mohm"]:
            return "Fair", "Internal resistance is usable but softer than a strong match battery."
        if resistance_mohm > r["excellent_max_mohm"]:
            return "Good", "Internal resistance looks healthy."
        return "Excellent", "Internal resistance looks strong."

    if resting_voltage_v is None:
        return None, None
    if resting_voltage_v < 12.1:
        return "Poor", "Resting voltage was low before or between load events."
    if resting_voltage_v < 12.4:
        return "Fair", "Resting voltage was only moderate."
    if resting_voltage_v < 12.65:
        return "Good", "Resting voltage looked healthy."
    return "Excellent", "Resting voltage looked strong."


def rate_load_assessment(
    current_stats: dict[str, float] | None,
    subsystem_current_stats: dict[str, dict[str, Any]],
    time_below_10v_s: float,
    brownout_events: int,
) -> tuple[str | None, str | None]:
    if current_stats is None:
        return None, None

    pack_p99 = current_stats["p99_a"]
    max_channel_p99 = max((stats["p99_a"] for stats in subsystem_current_stats.values()), default=0.0)

    if brownout_events > 0 or pack_p99 >= 200.0 or max_channel_p99 >= 110.0 or time_below_10v_s >= 15.0:
        return "Extreme", "The robot was under unusually heavy current demand."
    if pack_p99 >= 170.0 or max_channel_p99 >= 90.0 or time_below_10v_s >= 7.0:
        return "High", "The robot saw high current demand."
    if pack_p99 >= 130.0 or max_channel_p99 >= 65.0 or time_below_10v_s >= 2.0:
        return "Moderate", "The robot saw moderate current demand."
    return "Normal", "The robot load looked fairly normal."


def determine_dominant_cause(
    rating: str,
    battery_condition: str | None,
    load_assessment: str | None,
) -> str | None:
    if rating == "Unknown":
        return None
    if battery_condition == "Poor" and load_assessment in {"Normal", "Moderate", None}:
        return "battery"
    if battery_condition in {"Good", "Excellent"} and load_assessment in {"High", "Extreme"}:
        return "load"
    if battery_condition == "Poor" and load_assessment in {"High", "Extreme"}:
        return "mixed"
    if rating == "Critical" and load_assessment in {"High", "Extreme"} and battery_condition in {"Fair", "Good", "Excellent"}:
        return "load"
    if rating in {"Poor", "Critical"} and battery_condition in {"Poor", "Fair"}:
        return "battery"
    return "mixed"
