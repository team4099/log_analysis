from __future__ import annotations

import argparse
import json
import os
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
    "trace_export": {
        "enabled": True,
        "max_points": 400,
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
