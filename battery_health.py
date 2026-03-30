#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import json
import math
import os
import statistics
import struct
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "inputs": [],
    "output_path": None,
    "output_format": "auto",
    "jobs": max(1, (os.cpu_count() or 1) - 1),
    "entries": {
        "battery_voltage": "/SystemStats/BatteryVoltage",
        "browned_out": "/SystemStats/BrownedOut",
        "brownout_voltage": "/SystemStats/BrownoutVoltage",
        "enabled": "/DriverStation/Enabled",
        "autonomous": "/DriverStation/Autonomous",
    },
    "thresholds": {
        "min_enabled_duration_s_for_confidence_note": 5.0,
        "critical_min_voltage_v": 7.0,
        "critical_time_below_brownout_s": 0.1,
        "fallback_voltage": {
            "poor": {"p05_v": 9.0, "min_v": 8.5},
            "fair": {"p05_v": 10.0, "min_v": 9.5},
            "good": {"p05_v": 10.8, "min_v": 10.2},
        },
        "resistance": {
            "min_peak_current_a": 20.0,
            "excellent_max_mohm": 18.0,
            "good_max_mohm": 25.0,
            "fair_max_mohm": 35.0,
        },
    },
    "current_model": {
        "direct_supply_currents": [
            "/Climb/climbSupplyCurrent",
            "/Feeder/FeederSupplyCurrentAmps",
            "/Hopper/hopperSupplyCurrent",
            "/intake/intakeSupplyCurrentAmps",
            "/rollers/leaderSupplyCurrentAmps",
            "/rollers/followerSupplyCurrentAmps",
            "/Shooter/ShooterLeaderSupplyCurrent",
            "/Shooter/ShooterFollowerSupplyCurrent",
        ],
        "derived_supply_currents": [
            {
                "label": "Drive/Module0/drive",
                "stator_current": "/Drive/Module0/driveCurrent",
                "applied_voltage": "/Drive/Module0/driveAppliedVoltage",
            },
            {
                "label": "Drive/Module0/turn",
                "stator_current": "/Drive/Module0/turnCurrent",
                "applied_voltage": "/Drive/Module0/turnAppliedVoltage",
            },
            {
                "label": "Drive/Module1/drive",
                "stator_current": "/Drive/Module1/driveCurrent",
                "applied_voltage": "/Drive/Module1/driveAppliedVoltage",
            },
            {
                "label": "Drive/Module1/turn",
                "stator_current": "/Drive/Module1/turnCurrent",
                "applied_voltage": "/Drive/Module1/turnAppliedVoltage",
            },
            {
                "label": "Drive/Module2/drive",
                "stator_current": "/Drive/Module2/driveCurrent",
                "applied_voltage": "/Drive/Module2/driveAppliedVoltage",
            },
            {
                "label": "Drive/Module2/turn",
                "stator_current": "/Drive/Module2/turnCurrent",
                "applied_voltage": "/Drive/Module2/turnAppliedVoltage",
            },
            {
                "label": "Drive/Module3/drive",
                "stator_current": "/Drive/Module3/driveCurrent",
                "applied_voltage": "/Drive/Module3/driveAppliedVoltage",
            },
            {
                "label": "Drive/Module3/turn",
                "stator_current": "/Drive/Module3/turnCurrent",
                "applied_voltage": "/Drive/Module3/turnAppliedVoltage",
            },
        ],
        "subsystem_breakdown_currents": [
            {"label": "Drive/Module0/drive", "entry": "/Drive/Module0/driveCurrent", "current_type": "stator"},
            {"label": "Drive/Module0/turn", "entry": "/Drive/Module0/turnCurrent", "current_type": "stator"},
            {"label": "Drive/Module1/drive", "entry": "/Drive/Module1/driveCurrent", "current_type": "stator"},
            {"label": "Drive/Module1/turn", "entry": "/Drive/Module1/turnCurrent", "current_type": "stator"},
            {"label": "Drive/Module2/drive", "entry": "/Drive/Module2/driveCurrent", "current_type": "stator"},
            {"label": "Drive/Module2/turn", "entry": "/Drive/Module2/turnCurrent", "current_type": "stator"},
            {"label": "Drive/Module3/drive", "entry": "/Drive/Module3/driveCurrent", "current_type": "stator"},
            {"label": "Drive/Module3/turn", "entry": "/Drive/Module3/turnCurrent", "current_type": "stator"},
            {"label": "/Climb/climbStatorCurrent", "entry": "/Climb/climbStatorCurrent", "current_type": "stator"},
            {"label": "/Feeder/FeederStatorCurrentAmps", "entry": "/Feeder/FeederStatorCurrentAmps", "current_type": "stator"},
            {"label": "/Hopper/hopperStatorCurrent", "entry": "/Hopper/hopperStatorCurrent", "current_type": "stator"},
            {"label": "/intake/intakeStatorCurrentAmps", "entry": "/intake/intakeStatorCurrentAmps", "current_type": "stator"},
            {"label": "/rollers/leaderStatorCurrentAmps", "entry": "/rollers/leaderStatorCurrentAmps", "current_type": "stator"},
            {"label": "/rollers/followerStatorCurrentAmps", "entry": "/rollers/followerStatorCurrentAmps", "current_type": "stator"},
            {"label": "/Shooter/ShooterLeaderStatorCurrent", "entry": "/Shooter/ShooterLeaderStatorCurrent", "current_type": "stator"},
            {"label": "/Shooter/ShooterFollowerStatorCurrent", "entry": "/Shooter/ShooterFollowerStatorCurrent", "current_type": "stator"}
        ]
    },
}


@dataclass(frozen=True)
class LogRecord:
    entry: int
    timestamp_us: int
    data: bytes


@dataclass(frozen=True)
class EntryInfo:
    name: str
    type_name: str


@dataclass(frozen=True)
class BatterySummary:
    log_path: Path
    rating: str
    summary: str
    battery_condition: str | None
    battery_condition_summary: str | None
    load_assessment: str | None
    load_assessment_summary: str | None
    dominant_cause: str | None
    enabled_duration_s: float
    resting_voltage_v: float | None
    min_enabled_voltage_v: float | None
    p05_enabled_voltage_v: float | None
    brownout_voltage_v: float | None
    brownout_events: int
    time_below_brownout_s: float
    time_below_9v_s: float
    time_below_10v_s: float
    current_entry: str | None
    current_stats: dict[str, float] | None
    subsystem_current_stats: dict[str, dict[str, float]]
    peak_current_a: float | None
    internal_resistance_ohm: float | None
    phase_summaries: dict[str, dict[str, Any]]
    notes: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "log_path": str(self.log_path),
            "rating": self.rating,
            "summary": self.summary,
            "battery_condition": self.battery_condition,
            "battery_condition_summary": self.battery_condition_summary,
            "load_assessment": self.load_assessment,
            "load_assessment_summary": self.load_assessment_summary,
            "dominant_cause": self.dominant_cause,
            "enabled_duration_s": self.enabled_duration_s,
            "resting_voltage_v": self.resting_voltage_v,
            "min_enabled_voltage_v": self.min_enabled_voltage_v,
            "p05_enabled_voltage_v": self.p05_enabled_voltage_v,
            "brownout_voltage_v": self.brownout_voltage_v,
            "brownout_events": self.brownout_events,
            "time_below_brownout_s": self.time_below_brownout_s,
            "time_below_9v_s": self.time_below_9v_s,
            "time_below_10v_s": self.time_below_10v_s,
            "current_entry": self.current_entry,
            "current_stats": self.current_stats,
            "subsystem_current_stats": self.subsystem_current_stats,
            "peak_current_a": self.peak_current_a,
            "internal_resistance_ohm": self.internal_resistance_ohm,
            "phase_summaries": self.phase_summaries,
            "notes": self.notes,
        }


def build_error_summary(log_path: Path, message: str) -> BatterySummary:
    return BatterySummary(
        log_path=log_path,
        rating="Unknown",
        summary=message,
        battery_condition=None,
        battery_condition_summary=None,
        load_assessment=None,
        load_assessment_summary=None,
        dominant_cause=None,
        enabled_duration_s=0.0,
        resting_voltage_v=None,
        min_enabled_voltage_v=None,
        p05_enabled_voltage_v=None,
        brownout_voltage_v=None,
        brownout_events=0,
        time_below_brownout_s=0.0,
        time_below_9v_s=0.0,
        time_below_10v_s=0.0,
        current_entry=None,
        current_stats=None,
        subsystem_current_stats={},
        peak_current_a=None,
        internal_resistance_ohm=None,
        phase_summaries={},
        notes=[],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate battery health from WPILOG battery telemetry.",
    )
    parser.add_argument("paths", nargs="*", help="WPILOG files or directories.")
    parser.add_argument(
        "-i",
        "--input",
        dest="input_paths",
        action="append",
        default=[],
        help="Additional WPILOG file or directory input. Can be passed multiple times.",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        help="Path to config JSON. Defaults to battery_health_config.json next to this script.",
    )
    parser.add_argument("--json", action="store_true", help="Force JSON output.")
    parser.add_argument("-o", "--output", type=Path, help="Write results to a file.")
    parser.add_argument("-j", "--jobs", type=int, help="Number of workers to use across logs.")
    return parser.parse_args()


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(config_path: Path | None, script_dir: Path) -> tuple[dict[str, Any], Path]:
    resolved_path = (config_path or script_dir / "battery_health_config.json").expanduser().resolve()
    config = DEFAULT_CONFIG
    if resolved_path.exists():
        loaded = json.loads(resolved_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config root must be a JSON object")
        config = deep_merge(DEFAULT_CONFIG, loaded)
    return config, resolved_path


def apply_cli_overrides(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated = deep_merge({}, config)
    cli_inputs = [*args.paths, *args.input_paths]
    if cli_inputs:
        updated["inputs"] = cli_inputs
    if args.output is not None:
        updated["output_path"] = str(args.output)
    if args.jobs is not None:
        updated["jobs"] = args.jobs
    if args.json:
        updated["output_format"] = "json"
    return updated


def iter_records(raw: bytes) -> list[LogRecord]:
    if len(raw) < 12 or raw[:6] != b"WPILOG":
        raise ValueError("not a WPILOG file")

    header_size = struct.unpack_from("<I", raw, 8)[0]
    pos = 12 + header_size
    records: list[LogRecord] = []

    while pos < len(raw):
        length_byte = raw[pos]
        entry_len = (length_byte & 0x3) + 1
        size_len = ((length_byte >> 2) & 0x3) + 1
        timestamp_len = ((length_byte >> 4) & 0x7) + 1
        header_len = 1 + entry_len + size_len + timestamp_len

        entry = int.from_bytes(raw[pos + 1 : pos + 1 + entry_len], "little")
        size = int.from_bytes(raw[pos + 1 + entry_len : pos + 1 + entry_len + size_len], "little")
        timestamp_us = int.from_bytes(raw[pos + 1 + entry_len + size_len : pos + header_len], "little")
        data = raw[pos + header_len : pos + header_len + size]
        records.append(LogRecord(entry=entry, timestamp_us=timestamp_us, data=data))
        pos += header_len + size

    return records


def decode_control_start(data: bytes) -> tuple[int, EntryInfo] | None:
    if not data or data[0] != 0:
        return None

    offset = 1
    entry = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4

    name_len = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4
    name = data[offset : offset + name_len].decode("utf-8", "replace")
    offset += name_len

    type_len = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4
    type_name = data[offset : offset + type_len].decode("utf-8", "replace")
    offset += type_len

    metadata_len = int.from_bytes(data[offset : offset + 4], "little")
    offset += 4 + metadata_len

    if offset > len(data):
        return None

    return entry, EntryInfo(name=name, type_name=type_name)


def decode_value(type_name: str, data: bytes) -> Any:
    if type_name == "double":
        if len(data) != 8:
            raise ValueError(f"invalid double record length: {len(data)}")
        return struct.unpack("<d", data)[0]
    if type_name == "float":
        if len(data) != 4:
            raise ValueError(f"invalid float record length: {len(data)}")
        return struct.unpack("<f", data)[0]
    if type_name == "boolean":
        if len(data) != 1:
            raise ValueError(f"invalid boolean record length: {len(data)}")
        return bool(data[0])
    if type_name == "int64":
        if len(data) != 8:
            raise ValueError(f"invalid int64 record length: {len(data)}")
        return struct.unpack("<q", data)[0]
    if type_name == "string":
        return data.decode("utf-8", "replace")
    if type_name == "double[]":
        if len(data) % 8 != 0:
            raise ValueError("invalid double[] record length")
        count = len(data) // 8
        return list(struct.unpack(f"<{count}d", data))
    raise ValueError(f"unsupported type {type_name}")


def load_series(log_path: Path) -> dict[str, list[tuple[int, Any]]]:
    raw = log_path.read_bytes()
    entry_info: dict[int, EntryInfo] = {}
    series: dict[str, list[tuple[int, Any]]] = {}

    for record in iter_records(raw):
        if record.entry == 0:
            started = decode_control_start(record.data)
            if started is not None:
                entry, info = started
                entry_info[entry] = info
            continue

        info = entry_info.get(record.entry)
        if info is None:
            continue

        try:
            value = decode_value(info.type_name, record.data)
        except ValueError:
            continue

        series.setdefault(info.name, []).append((record.timestamp_us, value))

    return series


def expand_paths(items: list[str], script_dir: Path) -> list[Path]:
    if not items:
        items = [str(script_dir)]

    resolved: list[Path] = []
    for item in items:
        path = Path(item).expanduser()
        if path.is_dir():
            resolved.extend(sorted(path.glob("*.wpilog")))
        elif path.suffix == ".wpilog":
            resolved.append(path)

    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in resolved:
        resolved_path = path.resolve()
        if resolved_path not in seen:
            deduped.append(resolved_path)
            seen.add(resolved_path)

    return deduped


def split_series(series: list[tuple[int, Any]]) -> tuple[list[int], list[Any]]:
    return [timestamp for timestamp, _ in series], [value for _, value in series]


def value_at(timestamps: list[int], values: list[Any], timestamp_us: int, default: Any = None) -> Any:
    if not timestamps:
        return default
    index = bisect_right(timestamps, timestamp_us) - 1
    if index < 0:
        return default
    return values[index]


def state_at(series: list[tuple[int, Any]], timestamp_us: int, default: Any) -> Any:
    timestamps, values = split_series(series)
    return value_at(timestamps, values, timestamp_us, default)


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


def analyze_log(log_path: Path, config: dict[str, Any]) -> BatterySummary:
    entries = config["entries"]
    series = load_series(log_path)
    voltage_series = [
        (timestamp, float(value))
        for timestamp, value in series.get(entries["battery_voltage"], [])
        if isinstance(value, (int, float))
    ]
    enabled_series = [(timestamp, bool(value)) for timestamp, value in series.get(entries["enabled"], [])]
    autonomous_series = [(timestamp, bool(value)) for timestamp, value in series.get(entries["autonomous"], [])]
    browned_out_series = [(timestamp, bool(value)) for timestamp, value in series.get(entries["browned_out"], [])]
    brownout_voltage_series = [
        float(value)
        for _, value in series.get(entries["brownout_voltage"], [])
        if isinstance(value, (int, float))
    ]

    if not voltage_series:
        return build_error_summary(log_path, "Battery voltage entry is missing from this log.")

    current_entry, current_series, _, current_notes = build_estimated_current_series(series, voltage_series, config)
    subsystem_breakdown_series = build_subsystem_breakdown_series(series, config)

    brownout_voltage_v = brownout_voltage_series[-1] if brownout_voltage_series else None
    resting_voltage_v = estimate_resting_voltage(voltage_series, current_series, enabled_series)

    notes = list(current_notes)
    phase_summaries = {
        "all_enabled": build_phase_summary(
            resting_voltage_v,
            brownout_voltage_v,
            browned_out_series,
            voltage_series,
            current_series,
            subsystem_breakdown_series,
            lambda timestamp: state_at(enabled_series, timestamp, False),
            config,
        ),
        "auto": build_phase_summary(
            resting_voltage_v,
            brownout_voltage_v,
            browned_out_series,
            voltage_series,
            current_series,
            subsystem_breakdown_series,
            lambda timestamp: state_at(enabled_series, timestamp, False) and state_at(autonomous_series, timestamp, False),
            config,
        ),
        "teleop": build_phase_summary(
            resting_voltage_v,
            brownout_voltage_v,
            browned_out_series,
            voltage_series,
            current_series,
            subsystem_breakdown_series,
            lambda timestamp: state_at(enabled_series, timestamp, False) and not state_at(autonomous_series, timestamp, False),
            config,
        ),
    }

    overall_summary = phase_summaries["all_enabled"]
    enabled_duration_s = overall_summary["enabled_duration_s"]
    min_enabled_voltage_v = overall_summary["min_enabled_voltage_v"]
    p05_enabled_voltage_v = overall_summary["p05_enabled_voltage_v"]
    brownout_events = overall_summary["brownout_events"]
    time_below_brownout_s = overall_summary["time_below_brownout_s"]
    time_below_9v_s = overall_summary["time_below_9v_s"]
    time_below_10v_s = overall_summary["time_below_10v_s"]
    current_stats = overall_summary["current_stats"]
    subsystem_current_stats = overall_summary["subsystem_current_stats"]
    peak_current_a = overall_summary["peak_current_a"]
    internal_resistance_ohm = overall_summary["internal_resistance_ohm"]
    rating = overall_summary["rating"]
    summary = overall_summary["summary"]
    battery_condition = overall_summary["battery_condition"]
    battery_condition_summary = overall_summary["battery_condition_summary"]
    load_assessment = overall_summary["load_assessment"]
    load_assessment_summary = overall_summary["load_assessment_summary"]
    dominant_cause = overall_summary["dominant_cause"]

    resistance_thresholds = config["thresholds"]["resistance"]
    if current_stats is None or peak_current_a is None or peak_current_a < resistance_thresholds["min_peak_current_a"]:
        if current_entry is not None:
            notes.append("Estimated subsystem current never reached a convincing load level for resistance fitting.")
    elif internal_resistance_ohm is None:
        notes.append("Not enough loaded current samples to estimate internal resistance.")

    if enabled_duration_s < config["thresholds"]["min_enabled_duration_s_for_confidence_note"]:
        notes.append("Enabled runtime was short, so this score is based on limited load time.")

    return BatterySummary(
        log_path=log_path,
        rating=rating,
        summary=summary,
        battery_condition=battery_condition,
        battery_condition_summary=battery_condition_summary,
        load_assessment=load_assessment,
        load_assessment_summary=load_assessment_summary,
        dominant_cause=dominant_cause,
        enabled_duration_s=enabled_duration_s,
        resting_voltage_v=resting_voltage_v,
        min_enabled_voltage_v=min_enabled_voltage_v,
        p05_enabled_voltage_v=p05_enabled_voltage_v,
        brownout_voltage_v=brownout_voltage_v,
        brownout_events=brownout_events,
        time_below_brownout_s=time_below_brownout_s,
        time_below_9v_s=time_below_9v_s,
        time_below_10v_s=time_below_10v_s,
        current_entry=current_entry,
        current_stats=current_stats,
        subsystem_current_stats=subsystem_current_stats,
        peak_current_a=peak_current_a,
        internal_resistance_ohm=internal_resistance_ohm,
        phase_summaries=phase_summaries,
        notes=notes,
    )


def analyze_log_safe(log_path_str: str, config: dict[str, Any]) -> BatterySummary:
    log_path = Path(log_path_str)
    try:
        return analyze_log(log_path, config)
    except Exception as exc:
        return build_error_summary(log_path, f"Failed to analyze log: {exc}")


def format_optional_float(value: float | None, unit: str) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f} {unit}"


def render_text(summary: BatterySummary) -> str:
    lines = [
        f"{summary.log_path.name}: {summary.rating}",
        f"  {summary.summary}",
        f"  Battery condition: {summary.battery_condition or 'n/a'}",
        f"  Load assessment: {summary.load_assessment or 'n/a'}",
        f"  Dominant cause: {summary.dominant_cause or 'n/a'}",
        f"  Enabled time: {summary.enabled_duration_s:.1f} s",
        f"  Resting voltage estimate: {format_optional_float(summary.resting_voltage_v, 'V')}",
        f"  Enabled min voltage: {format_optional_float(summary.min_enabled_voltage_v, 'V')}",
        f"  Enabled 5th percentile voltage: {format_optional_float(summary.p05_enabled_voltage_v, 'V')}",
        f"  Brownout threshold: {format_optional_float(summary.brownout_voltage_v, 'V')}",
        f"  Brownout events: {summary.brownout_events}",
        f"  Time below brownout threshold: {summary.time_below_brownout_s:.2f} s",
        f"  Time below 9.0 V: {summary.time_below_9v_s:.2f} s",
        f"  Time below 10.0 V: {summary.time_below_10v_s:.2f} s",
    ]
    if summary.current_entry is not None:
        lines.append(f"  Current source: {summary.current_entry}")
    if summary.current_stats is not None:
        lines.append(
            "  Enabled current stats: "
            f"p50={summary.current_stats['p50_a']:.2f} A, "
            f"p90={summary.current_stats['p90_a']:.2f} A, "
            f"p95={summary.current_stats['p95_a']:.2f} A, "
            f"p99={summary.current_stats['p99_a']:.2f} A, "
            f"peak={summary.current_stats['peak_a']:.2f} A"
        )
    if summary.peak_current_a is not None:
        lines.append(f"  Peak current: {summary.peak_current_a:.2f} A")
    if summary.internal_resistance_ohm is not None:
        lines.append(f"  Estimated internal resistance: {summary.internal_resistance_ohm * 1000.0:.1f} mOhm")
    if summary.battery_condition_summary:
        lines.append(f"  Battery note: {summary.battery_condition_summary}")
    if summary.load_assessment_summary:
        lines.append(f"  Load note: {summary.load_assessment_summary}")
    if summary.subsystem_current_stats:
        lines.append("  Enabled subsystem current stats:")
        for name, stats in summary.subsystem_current_stats.items():
            lines.append(
                f"    {name} ({stats['current_type']}): "
                f"p50={stats['p50_a']:.2f} A, "
                f"p90={stats['p90_a']:.2f} A, "
                f"p95={stats['p95_a']:.2f} A, "
                f"p99={stats['p99_a']:.2f} A, peak={stats['peak_a']:.2f} A"
            )
    if summary.notes:
        lines.extend(f"  Note: {note}" for note in summary.notes)
    return "\n".join(lines)


def detect_output_format(config: dict[str, Any]) -> str:
    configured = str(config.get("output_format", "auto")).lower()
    if configured in {"json", "text"}:
        return configured
    output_path = config.get("output_path")
    if output_path and Path(output_path).suffix.lower() == ".json":
        return "json"
    return "text"


def analyze_logs(log_paths: list[Path], jobs: int, config: dict[str, Any]) -> list[BatterySummary]:
    if len(log_paths) <= 1 or jobs <= 1:
        return [analyze_log_safe(str(log_path), config) for log_path in log_paths]

    max_workers = min(len(log_paths), jobs)
    work_items = [str(path) for path in log_paths]
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(analyze_log_safe, work_items, itertools.repeat(config)))
    except PermissionError:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(analyze_log_safe, work_items, itertools.repeat(config)))


def emit_output(text: str, output_path: Path | None) -> None:
    if output_path is None:
        print(text)
        return
    output_path = output_path.expanduser()
    if output_path.parent != Path():
        output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")


def main() -> None:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    config, _ = load_config(args.config, script_dir)
    config = apply_cli_overrides(config, args)

    log_paths = expand_paths(list(config.get("inputs", [])), script_dir)
    if not log_paths:
        raise SystemExit("No .wpilog files found.")

    jobs = max(1, int(config.get("jobs", 1)))
    summaries = analyze_logs(log_paths, jobs, config)

    output_format = detect_output_format(config)
    output_path = Path(config["output_path"]).expanduser() if config.get("output_path") else None
    if output_format == "json":
        emit_output(json.dumps([summary.as_dict() for summary in summaries], indent=2), output_path)
        return

    emit_output("\n\n".join(render_text(summary) for summary in summaries), output_path)


if __name__ == "__main__":
    main()
