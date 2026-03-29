# Log Analysis

Battery log analysis tools for WPILOG files:

- `battery_health.py` analyzes one or more WPILOG files and produces a summary for each match.
- `battery_results_app.py` is a Streamlit viewer for browsing a generated `results.json`.
- `2026vache/results.json` is a checked-in sample dataset that the viewer can open by default, including from GitHub.

## What Each File Does

- `battery_health.py`: CLI analyzer that estimates battery condition, load severity, brownout behavior, pack current, and subsystem current summaries.
- `battery_health_config.json`: default analyzer config for telemetry entry names, thresholds, and current-model settings.
- `battery_results_app.py`: interactive Streamlit app for reviewing all analyzed logs and drilling into a single match.
- `battery_results_app_config.json`: viewer config, including the default GitHub `results.json` URL and display-name mappings.
- `2026vache/results.json`: sample output from the analyzer for the `2026vache` dataset. Teams can replace this with their own file or upload a different one in the viewer.

## Requirements

Python 3.10+ is recommended.

The analyzer only uses the standard library.

The viewer needs:

```bash
pip install streamlit pandas
```

## Analyzer Usage

Analyze one or more WPILOG files directly:

```bash
python3 battery_health.py path/to/log1.wpilog path/to/log2.wpilog --json -o results.json
```

Analyze an entire directory of `.wpilog` files:

```bash
python3 battery_health.py path/to/logs --json -o results.json
```

Use a custom config:

```bash
python3 battery_health.py path/to/logs --config battery_health_config.json --json -o results.json
```

Useful CLI flags:

- `--json`: force JSON output instead of text output
- `-o`, `--output`: write output to a file
- `-j`, `--jobs`: control parallel workers
- `-c`, `--config`: choose a config JSON
- `-i`, `--input`: add extra input paths

If no input path is provided, the analyzer searches the script directory for `.wpilog` files.

## Viewer Usage

Run the Streamlit app:

```bash
streamlit run battery_results_app.py
```

The sidebar supports three ways to load data:

1. Use the default GitHub-hosted `2026vache/results.json`
2. Enter a local file path or URL
3. Upload a `results.json` file directly

The app also has two views in the sidebar:

- `Fleet Overview`: default view for fleet-wide summary tables and charts
- `Match Detail`: drill into one selected match

GitHub URLs are supported in both forms:

- Raw URLs such as `https://raw.githubusercontent.com/.../2026vache/results.json`
- Standard GitHub blob URLs such as `https://github.com/.../blob/main/2026vache/results.json`

If the default GitHub URL is unavailable, the app falls back to the local checked-in `2026vache/results.json`.

The viewer defaults to the first match in the sorted dataset instead of a hardcoded match, and opens on the fleet-overview page by default.

## Expected `results.json` Workflow

Typical flow for a team member:

1. Run `battery_health.py` on their own WPILOG files.
2. Produce a JSON file, usually named `results.json`.
3. Either:
   - upload that file in the Streamlit app, or
   - commit/publish it somewhere and paste the local path or GitHub URL into the viewer.

Example:

```bash
python3 battery_health.py ~/logs/battery_session --json -o my_results.json
streamlit run battery_results_app.py
```

Then upload `my_results.json` in the app sidebar.

## Config Notes

### `battery_health_config.json`

Use this when your AdvantageKit entry names differ from the defaults, or if you want to tune:

- battery telemetry entry names
- brownout and voltage thresholds
- internal resistance thresholds
- direct and derived current sources
- subsystem breakdown channels

### `battery_results_app_config.json`

Use this to customize:

- default `results.json` source URL
- local fallback path
- display names shown in tables
- subsystem grouping rules

## GitHub Setup

This repo is configured so the Streamlit viewer defaults to:

[`https://raw.githubusercontent.com/team4099/log_analysis/main/2026vache/results.json`](https://raw.githubusercontent.com/team4099/log_analysis/main/2026vache/results.json)

That means once `2026vache/results.json` is pushed to the repository, the default viewer source works without any local path edits.

If you want the default source to point somewhere else, update `battery_results_app_config.json`.
