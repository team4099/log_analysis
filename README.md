# Log Analysis

Battery log analysis tools for WPILOG files:

- `battery_health.py` analyzes one or more WPILOG files and produces a summary for each match.
- `battery_results_app.py` is a Streamlit app for exploring analyzed match logs from a generated `results.json`.
- `logs/<example name>/results/results.json` is the standard dataset layout the viewer discovers automatically.

## What Each File Does

- `battery_health.py`: CLI analyzer that estimates battery condition, load severity, brownout behavior, pack current, and subsystem current summaries.
- `battery_health_config.json`: default analyzer config for telemetry entry names, thresholds, and current-model settings.
- `battery_results_app.py`: interactive Streamlit app for fleet-level review and single-match investigation.
- `battery_results_app_config.json`: viewer config, including the datasets root, default dataset name, and display-name mappings.
- `logs/2026vache/results/results.json`: sample output from the analyzer for the `2026vache` dataset.
- `logs/autos_03_29_26/results/results.json`: sample output for the `autos_03_29_26` dataset.

## Requirements

Python 3.10+ is recommended.

The analyzer only uses the standard library.

The viewer needs:

```bash
pip install streamlit pandas
```

## Analyzer Usage

Recommended repo layout:

```text
logs/
  my_example/
    match1.wpilog
    match2.wpilog
    results/
      results.json
```

Analyze one or more WPILOG files directly:

```bash
python3 battery_health.py path/to/log1.wpilog path/to/log2.wpilog --json -o logs/my_example/results/results.json
```

Analyze an entire directory of `.wpilog` files:

```bash
python3 battery_health.py logs/my_example --json -o logs/my_example/results/results.json
```

Use a custom config:

```bash
python3 battery_health.py logs/my_example --config battery_health_config.json --json -o logs/my_example/results/results.json
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

1. Use an example dataset discovered under `logs/<example name>/results/results.json`
2. Enter a local file path or URL
3. Upload a `results.json` file directly

When using example datasets, the app lets you choose between example names like `2026vache` or `autos_03_29_26`.

The app also has two views in the sidebar:

- `Fleet Overview`: default view for fleet-wide summary tables and charts
- `Match Detail`: drill into one selected match

GitHub URLs are supported in both forms:

- Raw URLs such as `https://raw.githubusercontent.com/.../logs/2026vache/results/results.json`
- Standard GitHub blob URLs such as `https://github.com/.../blob/main/logs/2026vache/results/results.json`

If the GitHub URL for the selected example is unavailable, the app falls back to the local checked-in `logs/<example name>/results/results.json`.

The viewer defaults to the first match in the sorted dataset instead of a hardcoded match, and opens on the fleet-overview page by default.

## Expected `results.json` Workflow

Typical flow for a team member:

1. Create a dataset folder under `logs/`, such as `logs/practice_04_05_26/`.
2. Put the `.wpilog` files directly in that folder.
3. Run `battery_health.py` and write output to `logs/practice_04_05_26/results/results.json`.
4. Open the Streamlit app and choose `practice_04_05_26` from the example selector.

Example:

```bash
mkdir -p logs/practice_04_05_26/results
python3 battery_health.py logs/practice_04_05_26 --json -o logs/practice_04_05_26/results/results.json
streamlit run battery_results_app.py
```

Then choose `practice_04_05_26` in the app sidebar.

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

- datasets root
- default dataset name
- GitHub logs base URL
- display names shown in tables
- subsystem grouping rules

## GitHub Setup

This repo is configured so the Streamlit viewer defaults to:

[`https://raw.githubusercontent.com/team4099/log_analysis/main/logs/2026vache/results/results.json`](https://raw.githubusercontent.com/team4099/log_analysis/main/logs/2026vache/results/results.json)

That means once `logs/<example name>/results/results.json` is pushed to the repository, that example can be selected in the viewer without any local path edits.

If you want the default source to point somewhere else, update `battery_results_app_config.json`.
