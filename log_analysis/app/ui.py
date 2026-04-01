from __future__ import annotations

import json
from typing import Any

import pandas as pd
from results_schema import parse_results_payload

from .comparison import (
    build_dataset_comparison_table,
    compare_subsystems_to_fleet,
    metric_delta_text,
    summarize_dataset,
)
from .data_source import (
    DEFAULT_APP_CONFIG_PATH,
    dataset_results_path,
    dataset_results_url,
    discover_datasets,
    load_app_config,
    load_results_for_dataset,
    load_results_from_source,
    resolve_config_path,
)
from .narrative import build_flags
from .normalize import (
    BATTERY_CONDITION_ORDER,
    DOMINANT_CAUSE_ORDER,
    PHASE_OPTIONS,
    RATING_ORDER,
    build_phase_dataframe,
    build_subsystem_frames_for_phase,
    display_name,
    extract_traces_by_log,
    has_phase_data,
    normalize_records,
    subsystem_dataframe,
    subsystem_map_for_phase,
    trace_dataframe,
)

try:
    import streamlit as st
except ImportError:  # pragma: no cover
    class _MissingStreamlit:
        def __getattr__(self, name: str) -> Any:
            raise RuntimeError(
                "streamlit is required to run the app UI. Install requirements.txt to use battery_results_app.py."
            )

    st = _MissingStreamlit()


def overview_column_config() -> dict[str, Any]:
    return {
        "match": st.column_config.TextColumn("Match", help="Match label parsed from the log filename."),
        "rating": st.column_config.TextColumn("Severity", help="Overall severity of the observed voltage behavior. This is not the same thing as battery quality."),
        "battery_condition": st.column_config.TextColumn("Battery", help="Estimated battery condition based mainly on effective resistance or resting voltage."),
        "load_assessment": st.column_config.TextColumn("Load", help="How hard the robot was pulling current in this match."),
        "dominant_cause": st.column_config.TextColumn("Cause", help="Best-effort attribution for whether the issue looks battery-driven, load-driven, or mixed."),
        "summary": st.column_config.TextColumn("Summary", help="Primary severity reason."),
        "min_enabled_voltage_v": st.column_config.NumberColumn("Min V", help="Minimum battery voltage while enabled in the selected phase or full match view.", format="%.2f V"),
        "p05_enabled_voltage_v": st.column_config.NumberColumn("P05 V", help="5th percentile battery voltage while enabled in the selected phase or full match view.", format="%.2f V"),
        "peak_current_a": st.column_config.NumberColumn("Peak Pack I", help="Peak estimated pack-side supply current while enabled. This is the whole-robot current model used for battery-health analysis.", format="%.1f A"),
        "current_p95_a": st.column_config.NumberColumn("P95 Pack I", help="95th percentile estimated pack-side supply current while enabled.", format="%.1f A"),
        "current_p99_a": st.column_config.NumberColumn("P99 Pack I", help="99th percentile estimated pack-side supply current while enabled.", format="%.1f A"),
        "current_p90_a": st.column_config.NumberColumn("P90 Pack I", help="90th percentile estimated pack-side supply current while enabled.", format="%.1f A"),
        "internal_resistance_mohm": st.column_config.NumberColumn("Reff", help="Estimated effective resistance over the selected phase or full match view. This is an in-match fit, not a fixed battery constant.", format="%.1f mOhm"),
        "brownout_events": st.column_config.NumberColumn("Brownouts", help="Number of browned-out transitions."),
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


def render_signed_delta_chart(comparison_table: pd.DataFrame) -> None:
    if not comparison_table.empty:
        st.bar_chart(comparison_table.set_index("subsystem_display")[["p99_delta_vs_peer_median"]])


def render_overview(
    df: pd.DataFrame,
    dataset_label: str = "Selected Dataset",
    comparison_df: pd.DataFrame | None = None,
    comparison_label: str | None = None,
) -> None:
    st.subheader("Fleet Overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Logs", len(df))
    c2.metric("Critical", int((df["rating"] == "Critical").sum()))
    c3.metric("Poor Batteries", int((df["battery_condition"] == "Poor").sum()))
    c4.metric("Load-Driven Criticals", int(((df["dominant_cause"] == "load") & (df["rating"] == "Critical")).sum()))

    severity_counts = df["rating"].astype(str).value_counts().reindex(RATING_ORDER, fill_value=0).rename_axis("severity").reset_index(name="count")
    battery_counts = df["battery_condition"].astype(str).value_counts().reindex(BATTERY_CONDITION_ORDER, fill_value=0).rename_axis("battery_condition").reset_index(name="count")
    cause_counts = df["dominant_cause"].astype(str).value_counts().reindex(DOMINANT_CAUSE_ORDER, fill_value=0).rename_axis("dominant_cause").reset_index(name="count")

    st.markdown("**Severity vs Battery Condition vs Cause**")
    a, b = st.columns(2)
    a.caption("Overall severity")
    a.bar_chart(severity_counts.set_index("severity"))
    b.caption("Battery condition")
    b.bar_chart(battery_counts.set_index("battery_condition"))
    st.caption("Dominant cause")
    st.bar_chart(cause_counts.set_index("dominant_cause"))

    st.dataframe(
        df[["match", "rating", "battery_condition", "dominant_cause", "summary", "min_enabled_voltage_v", "p05_enabled_voltage_v", "peak_current_a", "current_p95_a", "current_p99_a", "current_p90_a", "internal_resistance_mohm", "brownout_events"]],
        use_container_width=True,
        hide_index=True,
        column_config=overview_column_config(),
    )

    if comparison_df is not None and comparison_label is not None and not comparison_df.empty:
        st.markdown("**Dataset vs Dataset Comparison**")
        primary_summary = summarize_dataset(df)
        secondary_summary = summarize_dataset(comparison_df)

        d1, d2, d3, d4 = st.columns(4)
        d1.metric(
            f"{dataset_label} Logs",
            int(primary_summary["log_count"]),
            int(primary_summary["log_count"]) - int(secondary_summary["log_count"]),
        )
        d2.metric(
            f"{dataset_label} Criticals",
            int(primary_summary["critical_count"]),
            int(primary_summary["critical_count"]) - int(secondary_summary["critical_count"]),
        )
        d3.metric(
            "Median Min V",
            f"{primary_summary['median_min_enabled_voltage_v']:.2f} V"
            if primary_summary["median_min_enabled_voltage_v"] is not None
            else "n/a",
            metric_delta_text(
                primary_summary["median_min_enabled_voltage_v"],
                secondary_summary["median_min_enabled_voltage_v"],
            ),
        )
        d4.metric(
            "Median Reff",
            f"{primary_summary['median_internal_resistance_mohm']:.1f} mOhm"
            if primary_summary["median_internal_resistance_mohm"] is not None
            else "n/a",
            metric_delta_text(
                primary_summary["median_internal_resistance_mohm"],
                secondary_summary["median_internal_resistance_mohm"],
                inverse=True,
            ),
        )

        st.dataframe(
            build_dataset_comparison_table(df, comparison_df, dataset_label, comparison_label),
            use_container_width=True,
            hide_index=True,
        )


def render_match_page(
    df: pd.DataFrame,
    subsystem_stats: dict[str, dict[str, dict[str, float]]],
    phase_summaries_by_log: dict[str, dict[str, dict[str, Any]]],
    traces_by_log: dict[str, dict[str, Any]],
    app_config: dict[str, Any],
) -> None:
    st.subheader("Match Detail")
    phase_label = st.selectbox("Phase", list(PHASE_OPTIONS.keys()), index=0)
    phase_key = PHASE_OPTIONS[phase_label]
    if phase_key != "all_enabled" and not has_phase_data(phase_summaries_by_log, phase_key):
        st.info("This results file does not include phase-specific summaries yet. Re-run `battery_health.py` to enable auto/teleop filtering.")
        phase_key = "all_enabled"
    phase_df = build_phase_dataframe(df, phase_summaries_by_log, phase_key)
    all_subsystem_frames = build_subsystem_frames_for_phase(phase_df, subsystem_stats, phase_summaries_by_log, phase_key)
    selected_match = st.selectbox("Match", df["match"].tolist(), index=0)
    selected = phase_df[phase_df["match"] == selected_match].iloc[0]
    subsystem_map = subsystem_map_for_phase(selected["log_path"], subsystem_stats, phase_summaries_by_log, phase_key)
    render_selected_log(selected, phase_df, subsystem_map, all_subsystem_frames, traces_by_log, app_config)


def render_selected_log(
    selected: pd.Series,
    df: pd.DataFrame,
    subsystem_map: dict[str, dict[str, float]],
    all_subsystem_frames: dict[str, pd.DataFrame],
    traces_by_log: dict[str, dict[str, Any]],
    app_config: dict[str, Any],
) -> None:
    subsystem_df = subsystem_dataframe(subsystem_map)
    subsystem_comparison = compare_subsystems_to_fleet(selected["match"], subsystem_df, all_subsystem_frames)
    flags = build_flags(selected, df, subsystem_df, subsystem_comparison, app_config)

    st.subheader(f"Selected Log: {selected['match']}")
    st.caption(selected["log_path"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Severity", str(selected["rating"]))
    c2.metric("Battery Condition", str(selected["battery_condition"]), None)
    c3.metric("Dominant Cause", str(selected["dominant_cause"]), None)
    c4.metric("Brownouts", f"{int(selected['brownout_events'])}" if pd.notna(selected["brownout_events"]) else "n/a", None)

    e1, e2, e3, e4 = st.columns(4)
    e1.metric("Time Under Load", f"{selected['enabled_duration_s']:.1f} s" if pd.notna(selected["enabled_duration_s"]) else "n/a", None)
    e2.metric("Min Voltage", f"{selected['min_enabled_voltage_v']:.2f} V" if pd.notna(selected["min_enabled_voltage_v"]) else "n/a", metric_delta_text(selected["min_enabled_voltage_v"], df["min_enabled_voltage_v"].median()))
    e3.metric("P99 Pack Current", f"{selected['current_p99_a']:.1f} A" if pd.notna(selected["current_p99_a"]) else "n/a", metric_delta_text(selected["current_p99_a"], df["current_p99_a"].median()))
    e4.metric("Effective Resistance", f"{selected['internal_resistance_mohm']:.1f} mOhm" if pd.notna(selected["internal_resistance_mohm"]) else "n/a", metric_delta_text(selected["internal_resistance_mohm"], df["internal_resistance_mohm"].median(), inverse=True))

    st.markdown("**Interpretation**")
    st.write(f"- Battery condition: {selected.get('battery_condition_summary') or 'n/a'}")
    st.write(f"- Load assessment: {selected.get('load_assessment_summary') or 'n/a'}")
    st.caption("Voltage and effective resistance metrics are computed from the selected phase view, so they can differ between Auto and Teleop.")

    st.markdown("**What Went Wrong**")
    for flag in flags:
        st.write(f"- {flag}")

    st.markdown("**Whole-Robot Estimated Pack Current While Enabled**")
    st.caption("This is estimated pack-side supply current, meaning the battery-side current drawn by the robot as a whole. It is not motor stator current.")
    current_df = pd.DataFrame([{"metric": label, "amps": value} for label, value in [("p50 current", selected.get("current_p50_a")), ("p90 current", selected.get("current_p90_a")), ("p95 current", selected.get("current_p95_a")), ("p99 current", selected.get("current_p99_a")), ("peak current", selected.get("peak_current_a"))] if value is not None])
    if not current_df.empty:
        st.bar_chart(current_df.set_index("metric"))

    st.markdown("**Trace View**")
    trace_df = trace_dataframe(traces_by_log.get(selected["log_path"]))
    if trace_df.empty:
        st.info("No trace samples were exported for this results file.")
    else:
        trace_chart_df = trace_df.set_index("time_s")
        st.caption("Downsampled match trace for quick inspection of voltage, current, and robot state over time.")
        if trace_chart_df["voltage_v"].notna().any():
            st.markdown("Voltage vs Time")
            st.line_chart(trace_chart_df[["voltage_v"]])
        if trace_chart_df["pack_current_a"].notna().any():
            st.markdown("Estimated Pack Current vs Time")
            st.line_chart(trace_chart_df[["pack_current_a"]])
        state_columns = [column for column in ["enabled", "autonomous", "browned_out"] if column in trace_chart_df.columns]
        if state_columns:
            st.markdown("Robot State Timeline")
            st.line_chart(trace_chart_df[state_columns])

    st.markdown("**Per-Channel Enabled Current Breakdown**")
    if subsystem_df.empty:
        if (selected.get("enabled_duration_s") or 0.0) <= 0.0:
            st.info("This log has no enabled data in the selected phase.")
        else:
            st.info("No subsystem current breakdown was present in this results file.")
        return

    subsystem_table = subsystem_df.copy()
    subsystem_table["subsystem_display"] = subsystem_table["subsystem"].map(lambda name: display_name(name, app_config))
    st.dataframe(
        subsystem_table[["subsystem_display", "current_type", "source_entry", "family", "p50_a", "p90_a", "p95_a", "p99_a", "peak_a"]],
        use_container_width=True,
        hide_index=True,
        column_config=subsystem_column_config(),
    )
    st.bar_chart(subsystem_table.set_index("subsystem_display")[["p99_a"]])

    st.markdown("**Channels Higher Than Usual In This Match (Same Channel, Same Current Basis)**")
    st.caption("Positive values mean this channel drew more current than its fleet median. Negative values mean it drew less.")
    if subsystem_comparison.empty:
        st.info("No fleet comparison was available for subsystem currents.")
        return

    comparison_table = subsystem_comparison.copy()
    comparison_table["subsystem_display"] = comparison_table["subsystem"].map(lambda name: display_name(name, app_config))
    render_signed_delta_chart(comparison_table)
    st.dataframe(
        comparison_table[["subsystem_display", "current_type", "source_entry", "p90_a", "p95_a", "p99_a", "p95_percentile", "p99_percentile", "p95_delta_vs_peer_median", "p99_delta_vs_peer_median", "p90_delta_vs_peer_median"]],
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
        comparison_dataset = None
        path_text = ""
        uploaded = None
        source_label = ""

        if source_mode == "Example Dataset":
            if not dataset_names:
                st.warning(f"No datasets found under {datasets_root}.")
            else:
                default_index = dataset_names.index(default_dataset) if default_dataset in dataset_names else 0
                selected_dataset = st.selectbox("Example Name", dataset_names, index=default_index)
                comparison_options = ["None", *[name for name in dataset_names if name != selected_dataset]]
                comparison_choice = st.selectbox("Compare Against", comparison_options, index=0)
                comparison_dataset = None if comparison_choice == "None" else comparison_choice
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
        comparison_records = None
        if source_mode == "Upload":
            if uploaded is None:
                st.info("Upload a `results.json` file to inspect it.")
                st.stop()
            records = parse_results_payload(json.load(uploaded))
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
            records, _, _ = load_results_for_dataset(selected_dataset, datasets_root, app_config["github_logs_base_url"])
            if comparison_dataset is not None:
                comparison_records, _, _ = load_results_for_dataset(
                    comparison_dataset,
                    datasets_root,
                    app_config["github_logs_base_url"],
                )
    except Exception as exc:
        st.error(f"Failed to load results: {exc}")
        st.stop()

    st.caption(f"Loaded source: {source_label}")
    df, subsystem_stats, phase_summaries_by_log = normalize_records(records)
    traces_by_log = extract_traces_by_log(records)
    comparison_df = pd.DataFrame()
    comparison_label = None
    if comparison_records is not None:
        comparison_df, _, _ = normalize_records(comparison_records)
        comparison_label = comparison_dataset
    if df.empty:
        st.warning("No log summaries were found in this JSON file.")
        st.stop()

    with st.sidebar:
        st.header("View")
        page = st.radio("Page", ["Fleet Overview", "Match Detail"], index=0, label_visibility="collapsed")

    if page == "Fleet Overview":
        render_overview(
            df,
            selected_dataset or source_label,
            comparison_df if not comparison_df.empty else None,
            comparison_label,
        )
    else:
        render_match_page(df, subsystem_stats, phase_summaries_by_log, traces_by_log, app_config)
