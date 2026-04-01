from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from results_schema import dump_results_payload

from .battery_model import (
    build_estimated_current_series,
    build_phase_summary,
    build_trace_points,
    build_subsystem_breakdown_series,
    estimate_resting_voltage,
)
from .config import apply_cli_overrides, load_config
from .wpilog import expand_paths, load_series, state_at

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
    trace: dict[str, Any] | None
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
            "trace": self.trace,
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
        trace=None,
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

    trace_config = config.get("trace_export", {})
    trace = None
    if trace_config.get("enabled", True):
        trace = build_trace_points(
            voltage_series,
            current_series,
            enabled_series,
            autonomous_series,
            browned_out_series,
            max(2, int(trace_config.get("max_points", 400))),
        )

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
        trace=trace,
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
        emit_output(dump_results_payload([summary.as_dict() for summary in summaries], log_paths, output_path), output_path)
        return

    emit_output("\n\n".join(render_text(summary) for summary in summaries), output_path)
