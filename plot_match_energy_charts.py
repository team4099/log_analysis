#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from log_analysis.battery_model import build_estimated_current_series
from log_analysis.config import load_config
from log_analysis.wpilog import expand_paths, load_series, split_series, state_at, value_at
from log_analysis.app.normalize import get_match_label, match_sort_key


DEFAULT_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
    "#4e79a7",
    "#f28e2b",
    "#59a14f",
    "#e15759",
    "#76b7b2",
    "#edc948",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate match-level supply current and energy charts for a WPILOG dataset.",
    )
    parser.add_argument("dataset", type=Path, help="Directory containing .wpilog files.")
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        help="Path to the analyzer config JSON. Defaults to battery_health_config.json next to this script.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        help="Directory to write chart PNGs into. Defaults to <dataset>/results.",
    )
    return parser.parse_args()


def build_unique_match_labels(log_paths: list[Path]) -> dict[Path, str]:
    base_labels = {path: get_match_label(str(path)).upper() for path in log_paths}
    counts = Counter(base_labels.values())
    unique_labels: dict[Path, str] = {}

    for path in log_paths:
        label = base_labels[path]
        if counts[label] == 1:
            unique_labels[path] = label
            continue
        parts = path.stem.split("_")
        time_part = parts[2] if len(parts) >= 3 else path.stem
        unique_labels[path] = f"{label}\n{time_part.replace('-', ':')[:5]}"
    return unique_labels


def load_display_names(script_dir: Path) -> dict[str, str]:
    config_path = script_dir / "battery_results_app_config.json"
    if not config_path.exists():
        return {}
    try:
        loaded = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    display_names = loaded.get("display_names")
    return display_names if isinstance(display_names, dict) else {}


def component_order(config: dict[str, Any], metrics: list[dict[str, Any]]) -> list[str]:
    ordered = [
        entry["label"]
        for entry in config.get("current_model", {}).get("subsystem_breakdown_currents", [])
        if isinstance(entry, dict) and isinstance(entry.get("label"), str)
    ]
    seen = set(ordered)
    for metric in metrics:
        for component_name in metric["energy_wh"]:
            if component_name not in seen:
                ordered.append(component_name)
                seen.add(component_name)
        for component_name in metric["mean_supply_current_a"]:
            if component_name not in seen:
                ordered.append(component_name)
                seen.add(component_name)
    return ordered


def display_name(component_name: str, display_names: dict[str, str]) -> str:
    return display_names.get(component_name) or component_name.lstrip("/")


def component_color_map(component_names: list[str]) -> dict[str, str]:
    return {
        component_name: DEFAULT_COLORS[index % len(DEFAULT_COLORS)]
        for index, component_name in enumerate(component_names)
    }


def supply_label_aliases(config: dict[str, Any]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    stator_entries = {
        entry["label"]: entry["entry"]
        for entry in config.get("current_model", {}).get("subsystem_breakdown_currents", [])
        if isinstance(entry, dict) and isinstance(entry.get("label"), str) and isinstance(entry.get("entry"), str)
    }
    for label, stator_entry in stator_entries.items():
        if "StatorCurrent" in stator_entry:
            aliases[stator_entry.replace("StatorCurrent", "SupplyCurrent")] = label
        if "statorCurrent" in stator_entry:
            aliases[stator_entry.replace("statorCurrent", "SupplyCurrent")] = label
    return aliases


def component_current_at(
    lookup: tuple[list[int], list[float]],
    timestamp_us: int,
) -> float:
    timestamps, values = lookup
    value = value_at(timestamps, values, timestamp_us, default=0.0)
    if isinstance(value, (int, float)):
        return max(0.0, float(value))
    return 0.0


def summarize_log_metrics(log_path: Path, config: dict[str, Any]) -> dict[str, Any]:
    entries = config["entries"]
    series = load_series(log_path)
    voltage_series = [
        (timestamp, float(value))
        for timestamp, value in series.get(entries["battery_voltage"], [])
        if isinstance(value, (int, float))
    ]
    enabled_series = [(timestamp, bool(value)) for timestamp, value in series.get(entries["enabled"], [])]
    if len(voltage_series) < 2:
        return {
            "log_path": log_path,
            "mean_supply_current_a": {},
            "energy_wh": {},
            "enabled_duration_s": 0.0,
        }

    _, _, supply_component_series, _ = build_estimated_current_series(series, voltage_series, config)
    supply_aliases = supply_label_aliases(config)
    normalized_supply_series: dict[str, list[tuple[int, float]]] = {}
    for component_name, component_values in supply_component_series.items():
        normalized_name = supply_aliases.get(component_name, component_name)
        normalized_supply_series[normalized_name] = component_values
    supply_component_lookups = {
        component_name: split_series(component_values)
        for component_name, component_values in normalized_supply_series.items()
        if component_values
    }

    supply_current_area_by_component = {component_name: 0.0 for component_name in supply_component_lookups}
    energy_wh_by_component = {component_name: 0.0 for component_name in supply_component_lookups}
    enabled_duration_s = 0.0

    for (t0, v0), (t1, v1) in zip(voltage_series, voltage_series[1:]):
        # Only accumulate current and energy over intervals where the robot stayed enabled.
        if not (state_at(enabled_series, t0, False) and state_at(enabled_series, t1, False)):
            continue
        dt_s = max(0.0, (t1 - t0) / 1_000_000.0)
        if dt_s <= 0.0:
            continue
        enabled_duration_s += dt_s
        for component_name, lookup in supply_component_lookups.items():
            current0_a = component_current_at(lookup, t0)
            current1_a = component_current_at(lookup, t1)
            supply_current_area_by_component[component_name] += ((current0_a + current1_a) / 2.0) * dt_s
            avg_power_w = ((v0 * current0_a) + (v1 * current1_a)) / 2.0
            energy_wh_by_component[component_name] += avg_power_w * (dt_s / 3600.0)

    if enabled_duration_s > 0.0:
        mean_supply_current_a = {
            component_name: supply_current_area_by_component[component_name] / enabled_duration_s
            for component_name in supply_component_lookups
        }
    else:
        mean_supply_current_a = {component_name: 0.0 for component_name in supply_component_lookups}

    return {
        "log_path": log_path,
        "mean_supply_current_a": mean_supply_current_a,
        "energy_wh": energy_wh_by_component,
        "enabled_duration_s": enabled_duration_s,
    }


def plot_heatmap(
    match_labels: list[str],
    metrics: list[dict[str, Any]],
    component_names: list[str],
    display_names: dict[str, str],
    output_path: Path,
) -> None:
    data = [
        [metric["mean_supply_current_a"].get(component_name, 0.0) for component_name in component_names]
        for metric in metrics
    ]
    row_count = max(1, len(match_labels))
    fig_width = max(14.0, 0.9 * len(component_names) + 4.0)
    fig_height = max(5.0, 0.7 * row_count + 2.5)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    image = ax.imshow(data, aspect="auto", cmap="YlOrRd")
    colorbar = fig.colorbar(image, ax=ax)
    colorbar.set_label("Mean Enabled Supply Current (A)")

    ax.set_title("Mean Enabled Supply Current by Motor per Match", fontsize=18, weight="bold", pad=16)
    ax.set_xticks(range(len(component_names)))
    ax.set_xticklabels([display_name(name, display_names) for name in component_names], rotation=60, ha="right")
    ax.set_yticks(range(len(match_labels)))
    ax.set_yticklabels(match_labels)

    max_value = max((value for row in data for value in row), default=0.0)
    threshold = max_value * 0.55
    for row_index, row in enumerate(data):
        for column_index, value in enumerate(row):
            text_color = "white" if value >= threshold and max_value > 0 else "#222222"
            ax.text(
                column_index,
                row_index,
                f"{value:.1f}",
                ha="center",
                va="center",
                color=text_color,
                fontsize=10,
            )

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_energy_bars(
    match_labels: list[str],
    metrics: list[dict[str, Any]],
    component_names: list[str],
    display_names: dict[str, str],
    output_path: Path,
) -> None:
    fig_width = max(12.0, 0.85 * len(match_labels) + 5.0)
    fig, ax = plt.subplots(figsize=(fig_width, 7.5))
    x_positions = list(range(len(match_labels)))
    bottoms = [0.0] * len(match_labels)
    totals = [sum(metric["energy_wh"].get(component_name, 0.0) for component_name in component_names) for metric in metrics]
    colors = component_color_map(component_names)

    for component_name in component_names:
        values = [metric["energy_wh"].get(component_name, 0.0) for metric in metrics]
        ax.bar(
            x_positions,
            values,
            bottom=bottoms,
            label=display_name(component_name, display_names),
            color=colors[component_name],
            edgecolor="#333333",
            linewidth=0.3,
        )
        bottoms = [bottom + value for bottom, value in zip(bottoms, values)]

    for index, total in enumerate(totals):
        ax.text(index, total + max(totals, default=0.0) * 0.01, f"{total:.1f}", ha="center", va="bottom", fontsize=10)

    ax.set_title("Enabled Energy Consumption per Match by Motor", fontsize=18, weight="bold", pad=10)
    ax.set_ylabel("Total Energy (Wh)")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(match_labels, rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    config, _ = load_config(args.config, script_dir)
    display_names = load_display_names(script_dir)

    dataset_path = args.dataset.expanduser().resolve()
    output_dir = (args.output_dir or dataset_path / "results").expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    log_paths = expand_paths([str(dataset_path)], script_dir)
    if not log_paths:
        raise SystemExit(f"No .wpilog files found in {dataset_path}.")

    sorted_log_paths = sorted(
        log_paths,
        key=lambda path: (match_sort_key(get_match_label(str(path))), path.name),
    )
    unique_labels = build_unique_match_labels(sorted_log_paths)
    metrics = [summarize_log_metrics(path, config) for path in sorted_log_paths]
    component_names = component_order(config, metrics)
    match_labels = [unique_labels[path] for path in sorted_log_paths]

    heatmap_path = output_dir / "supply_current_heatmap.png"
    energy_path = output_dir / "power_consumption_per_match.png"
    plot_heatmap(match_labels, metrics, component_names, display_names, heatmap_path)
    plot_energy_bars(match_labels, metrics, component_names, display_names, energy_path)

    print(heatmap_path)
    print(energy_path)


if __name__ == "__main__":
    main()
