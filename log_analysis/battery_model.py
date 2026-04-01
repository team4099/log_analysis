from __future__ import annotations

import math
import statistics
from typing import Any

from .scoring import (
    determine_dominant_cause,
    rate_battery,
    rate_battery_condition,
    rate_load_assessment,
)
from .wpilog import split_series, state_at, value_at


def build_trace_points(
    voltage_series: list[tuple[int, float]],
    current_series: list[tuple[int, float]],
    enabled_series: list[tuple[int, bool]],
    autonomous_series: list[tuple[int, bool]],
    browned_out_series: list[tuple[int, bool]],
    max_points: int,
) -> dict[str, Any] | None:
    if not voltage_series:
        return None

    total_points = len(voltage_series)
    if total_points <= 1:
        sampled_points = voltage_series
    else:
        sample_count = max(2, min(total_points, max_points))
        if sample_count >= total_points:
            sampled_points = voltage_series
        else:
            sampled_indices = sorted(
                {
                    min(
                        total_points - 1,
                        round(index * (total_points - 1) / (sample_count - 1)),
                    )
                    for index in range(sample_count)
                }
            )
            sampled_points = [voltage_series[index] for index in sampled_indices]

    current_lookup = split_series(current_series) if current_series else None
    start_timestamp_us = voltage_series[0][0]
    end_timestamp_us = voltage_series[-1][0]
    trace_points = []
    for timestamp_us, voltage_v in sampled_points:
        pack_current_a = None
        if current_lookup is not None:
            lookup_value = value_at(*current_lookup, timestamp_us, default=None)
            if isinstance(lookup_value, (int, float)):
                pack_current_a = float(lookup_value)
        trace_points.append(
            {
                "time_s": max(0.0, (timestamp_us - start_timestamp_us) / 1_000_000.0),
                "voltage_v": voltage_v,
                "pack_current_a": pack_current_a,
                "enabled": state_at(enabled_series, timestamp_us, False),
                "autonomous": state_at(autonomous_series, timestamp_us, False),
                "browned_out": state_at(browned_out_series, timestamp_us, False),
            }
        )

    return {
        "duration_s": max(0.0, (end_timestamp_us - start_timestamp_us) / 1_000_000.0),
        "sample_count": len(sampled_points),
        "points": trace_points,
    }

def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile of empty sequence")
    if len(values) == 1:
        return values[0]

    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def align_numeric_series(
    base: list[tuple[int, float]],
    other: list[tuple[int, float]],
) -> list[tuple[int, float, float | None]]:
    if not base:
        return []
    if not other:
        return [(timestamp, value, None) for timestamp, value in base]

    aligned: list[tuple[int, float, float | None]] = []
    other_index = 0

    for timestamp, value in base:
        while other_index + 1 < len(other) and other[other_index + 1][0] <= timestamp:
            other_index += 1

        if other[other_index][0] > timestamp:
            aligned.append((timestamp, value, None))
            continue

        if other_index + 1 < len(other) and other[other_index + 1][0] > timestamp:
            t0, v0 = other[other_index]
            t1, v1 = other[other_index + 1]
            ratio = (timestamp - t0) / (t1 - t0)
            aligned_value = v0 + (v1 - v0) * ratio
        else:
            aligned_value = other[other_index][1]

        aligned.append((timestamp, value, aligned_value))

    return aligned


def count_true_transitions(series: list[tuple[int, bool]]) -> int:
    count = 0
    previous = False
    for _, value in series:
        if value and not previous:
            count += 1
        previous = value
    return count


def count_true_transitions_for_filter(
    series: list[tuple[int, bool]],
    include_timestamp: callable,
) -> int:
    filtered = [(timestamp, value) for timestamp, value in series if include_timestamp(timestamp)]
    return count_true_transitions(filtered)


def integrate_below_threshold(enabled_points: list[tuple[int, float]], threshold: float) -> float:
    if len(enabled_points) < 2:
        return 0.0

    total_s = 0.0
    for (t0, v0), (t1, v1) in zip(enabled_points, enabled_points[1:]):
        if v0 < threshold and v1 < threshold:
            total_s += (t1 - t0) / 1_000_000.0
    return total_s


def build_estimated_current_series(
    series: dict[str, list[tuple[int, Any]]],
    voltage_series: list[tuple[int, float]],
    config: dict[str, Any],
) -> tuple[str | None, list[tuple[int, float]], dict[str, list[tuple[int, float]]], list[str]]:
    if not voltage_series:
        return None, [], {}, ["Battery voltage data is missing, so subsystem current could not be estimated."]

    current_model = config["current_model"]
    direct_entries = current_model["direct_supply_currents"]
    derived_entries = current_model["derived_supply_currents"]

    notes: list[str] = []
    lookups: dict[str, tuple[list[int], list[float]]] = {}
    missing_direct: list[str] = []
    missing_derived: list[str] = []
    component_series: dict[str, list[tuple[int, float]]] = {}

    for entry in direct_entries:
        numeric_series = [
            (timestamp, float(value))
            for timestamp, value in series.get(entry, [])
            if isinstance(value, (int, float))
        ]
        if numeric_series:
            lookups[entry] = split_series(numeric_series)
        else:
            missing_direct.append(entry)

    for derived in derived_entries:
        stator_name = derived["stator_current"]
        applied_name = derived["applied_voltage"]
        stator_series = [
            (timestamp, float(value))
            for timestamp, value in series.get(stator_name, [])
            if isinstance(value, (int, float))
        ]
        applied_series = [
            (timestamp, float(value))
            for timestamp, value in series.get(applied_name, [])
            if isinstance(value, (int, float))
        ]
        if stator_series and applied_series:
            lookups[stator_name] = split_series(stator_series)
            lookups[applied_name] = split_series(applied_series)
        else:
            missing_derived.append(derived["label"])

    estimated_current: list[tuple[int, float]] = []
    for timestamp_us, battery_voltage_v in voltage_series:
        total_current_a = 0.0

        for entry in direct_entries:
            lookup = lookups.get(entry)
            if lookup is None:
                continue
            value = value_at(*lookup, timestamp_us, default=None)
            if isinstance(value, (int, float)):
                component_current_a = max(0.0, float(value))
                total_current_a += component_current_a
                component_series.setdefault(entry, []).append((timestamp_us, component_current_a))

        if battery_voltage_v > 1.0:
            for derived in derived_entries:
                stator_lookup = lookups.get(derived["stator_current"])
                applied_lookup = lookups.get(derived["applied_voltage"])
                if stator_lookup is None or applied_lookup is None:
                    continue
                stator_current_a = value_at(*stator_lookup, timestamp_us, default=None)
                applied_voltage_v = value_at(*applied_lookup, timestamp_us, default=None)
                if not isinstance(stator_current_a, (int, float)) or not isinstance(applied_voltage_v, (int, float)):
                    continue

                duty_cycle = min(abs(float(applied_voltage_v)) / battery_voltage_v, 1.0)
                component_current_a = max(0.0, abs(float(stator_current_a)) * duty_cycle)
                total_current_a += component_current_a
                component_series.setdefault(derived["label"], []).append((timestamp_us, component_current_a))

        estimated_current.append((timestamp_us, total_current_a))

    if missing_direct:
        notes.append(f"Missing direct supply channels: {', '.join(missing_direct)}")
    if missing_derived:
        notes.append(f"Missing derived channels: {', '.join(missing_derived)}")

    return "estimated_subsystem_sum", estimated_current, component_series, notes


def summarize_current_series(series: list[tuple[int, float]]) -> dict[str, float] | None:
    if not series:
        return None

    values = [value for _, value in series]
    return {
        "p50_a": percentile(values, 0.50),
        "p90_a": percentile(values, 0.90),
        "p95_a": percentile(values, 0.95),
        "p99_a": percentile(values, 0.99),
        "peak_a": max(values),
    }


def build_subsystem_breakdown_series(
    series: dict[str, list[tuple[int, Any]]],
    config: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    breakdown: dict[str, dict[str, Any]] = {}
    for entry_config in config["current_model"].get("subsystem_breakdown_currents", []):
        numeric_series = [
            (timestamp, float(value))
            for timestamp, value in series.get(entry_config["entry"], [])
            if isinstance(value, (int, float))
        ]
        if numeric_series:
            breakdown[entry_config["label"]] = {
                "source_entry": entry_config["entry"],
                "current_type": entry_config["current_type"],
                "series": numeric_series,
            }
    return breakdown


def summarize_enabled_component_currents(
    component_series: dict[str, dict[str, Any]],
    enabled_series: list[tuple[int, bool]],
) -> dict[str, dict[str, Any]]:
    return summarize_component_currents_for_filter(
        component_series,
        lambda timestamp: state_at(enabled_series, timestamp, False),
    )


def summarize_component_currents_for_filter(
    component_series: dict[str, dict[str, Any]],
    include_timestamp: callable,
) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for name, component in component_series.items():
        series = component["series"]
        enabled_only = [
            (timestamp, value)
            for timestamp, value in series
            if include_timestamp(timestamp)
        ]
        summary = summarize_current_series(enabled_only)
        if summary is not None:
            summaries[name] = {
                **summary,
                "current_type": component["current_type"],
                "source_entry": component["source_entry"],
            }
    return dict(sorted(summaries.items(), key=lambda item: item[1]["p99_a"], reverse=True))


def estimate_internal_resistance(
    resting_voltage_v: float | None,
    enabled_points: list[tuple[int, float, float | None]],
) -> float | None:
    if resting_voltage_v is None:
        return None

    samples: list[float] = []
    for _, voltage_v, current_a in enabled_points:
        if current_a is None or current_a <= 15.0:
            continue
        if voltage_v > resting_voltage_v:
            continue
        samples.append((resting_voltage_v - voltage_v) / current_a)

    if len(samples) < 5:
        return None
    return statistics.median(samples)


def estimate_resting_voltage(
    voltage_series: list[tuple[int, float]],
    current_series: list[tuple[int, float]],
    enabled_series: list[tuple[int, bool]],
) -> float | None:
    disabled_voltage_values = [
        voltage
        for timestamp, voltage in voltage_series
        if not state_at(enabled_series, timestamp, False)
    ]
    if not disabled_voltage_values:
        disabled_voltage_values = [voltage for _, voltage in voltage_series]

    if current_series:
        aligned = align_numeric_series(voltage_series, current_series)
        quiet_disabled_voltages = [
            voltage
            for timestamp, voltage, current in aligned
            if current is not None
            and current <= 5.0
            and not state_at(enabled_series, timestamp, False)
        ]
        if len(quiet_disabled_voltages) >= 10:
            return percentile(quiet_disabled_voltages, 0.9)

    if len(disabled_voltage_values) < 5:
        return max(disabled_voltage_values) if disabled_voltage_values else None
    return percentile(disabled_voltage_values, 0.9)

def build_phase_summary(
    resting_voltage_v: float | None,
    brownout_voltage_v: float | None,
    browned_out_series: list[tuple[int, bool]],
    voltage_series: list[tuple[int, float]],
    current_series: list[tuple[int, float]],
    subsystem_breakdown_series: dict[str, dict[str, Any]],
    include_timestamp: callable,
    config: dict[str, Any],
) -> dict[str, Any]:
    enabled_points = [
        (timestamp, voltage)
        for timestamp, voltage in voltage_series
        if include_timestamp(timestamp)
    ]
    enabled_duration_s = 0.0
    if len(enabled_points) >= 2:
        enabled_duration_s = (enabled_points[-1][0] - enabled_points[0][0]) / 1_000_000.0

    enabled_voltages = [voltage for _, voltage in enabled_points]
    min_enabled_voltage_v = min(enabled_voltages) if enabled_voltages else None
    p05_enabled_voltage_v = percentile(enabled_voltages, 0.05) if enabled_voltages else None
    brownout_events = count_true_transitions_for_filter(browned_out_series, include_timestamp)
    enabled_current_series = [
        (timestamp, current)
        for timestamp, current in current_series
        if include_timestamp(timestamp)
    ]
    current_stats = summarize_current_series(enabled_current_series)
    enabled_current_values = [current for _, current in enabled_current_series]
    peak_current_a = current_stats["peak_a"] if current_stats is not None else None
    subsystem_current_stats = summarize_component_currents_for_filter(subsystem_breakdown_series, include_timestamp)
    aligned_enabled_points = [
        point
        for point in align_numeric_series(voltage_series, current_series)
        if include_timestamp(point[0])
    ]

    resistance_thresholds = config["thresholds"]["resistance"]
    if enabled_current_values and peak_current_a is not None and peak_current_a >= resistance_thresholds["min_peak_current_a"]:
        internal_resistance_ohm = estimate_internal_resistance(resting_voltage_v, aligned_enabled_points)
    else:
        internal_resistance_ohm = None

    time_below_brownout_s = (
        integrate_below_threshold(enabled_points, brownout_voltage_v)
        if brownout_voltage_v is not None
        else 0.0
    )
    time_below_9v_s = integrate_below_threshold(enabled_points, 9.0)
    time_below_10v_s = integrate_below_threshold(enabled_points, 10.0)

    rating, summary = rate_battery(
        min_enabled_voltage_v,
        p05_enabled_voltage_v,
        brownout_events,
        time_below_brownout_s,
        internal_resistance_ohm,
        config,
    )
    battery_condition, battery_condition_summary = rate_battery_condition(
        internal_resistance_ohm,
        resting_voltage_v,
        config,
    )
    load_assessment, load_assessment_summary = rate_load_assessment(
        current_stats,
        subsystem_current_stats,
        time_below_10v_s,
        brownout_events,
    )
    dominant_cause = determine_dominant_cause(rating, battery_condition, load_assessment)

    return {
        "rating": rating,
        "summary": summary,
        "battery_condition": battery_condition,
        "battery_condition_summary": battery_condition_summary,
        "load_assessment": load_assessment,
        "load_assessment_summary": load_assessment_summary,
        "dominant_cause": dominant_cause,
        "enabled_duration_s": enabled_duration_s,
        "min_enabled_voltage_v": min_enabled_voltage_v,
        "p05_enabled_voltage_v": p05_enabled_voltage_v,
        "brownout_events": brownout_events,
        "time_below_brownout_s": time_below_brownout_s,
        "time_below_9v_s": time_below_9v_s,
        "time_below_10v_s": time_below_10v_s,
        "current_stats": current_stats,
        "subsystem_current_stats": subsystem_current_stats,
        "peak_current_a": peak_current_a,
        "internal_resistance_ohm": internal_resistance_ohm,
    }
