from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

from results_schema import parse_results_payload


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASETS_ROOT = REPO_ROOT / "logs"
DEFAULT_APP_CONFIG_PATH = REPO_ROOT / "battery_results_app_config.json"
DEFAULT_GITHUB_LOGS_BASE_URL = "https://raw.githubusercontent.com/team4099/log_analysis/main/logs"
DEFAULT_APP_CONFIG: dict[str, Any] = {
    "datasets_root": str(DEFAULT_DATASETS_ROOT),
    "default_dataset": "autos_dp_03_30_26",
    "github_logs_base_url": DEFAULT_GITHUB_LOGS_BASE_URL,
    "comparison_metric": "p99_delta_vs_peer_median",
    "comparison_top_n": 8,
    "display_names": {},
    "subsystem_groups": [
        {"name": "Module 0", "patterns": ["Drive/Module0/drive", "Drive/Module0/turn"]},
        {"name": "Module 1", "patterns": ["Drive/Module1/drive", "Drive/Module1/turn"]},
        {"name": "Module 2", "patterns": ["Drive/Module2/drive", "Drive/Module2/turn"]},
        {"name": "Module 3", "patterns": ["Drive/Module3/drive", "Drive/Module3/turn"]},
        {"name": "Shooter", "patterns": ["/Shooter/ShooterLeaderStatorCurrent", "/Shooter/ShooterFollowerStatorCurrent"]},
        {"name": "Feed", "patterns": ["/Feeder/FeederStatorCurrentAmps", "/Hopper/hopperStatorCurrent", "/rollers/leaderStatorCurrentAmps", "/rollers/followerStatorCurrentAmps"]},
        {"name": "Intake", "patterns": ["/intake/intakeStatorCurrentAmps"]},
        {"name": "Climb", "patterns": ["/Climb/climbStatorCurrent"]},
    ],
}


def load_results(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return parse_results_payload(raw)


def resolve_config_path(value: str, config_path: Path) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (config_path.parent / candidate).resolve()


def dataset_results_path(datasets_root: Path, dataset_name: str) -> Path:
    return datasets_root / dataset_name / "results" / "results.json"


def dataset_results_url(base_url: str, dataset_name: str) -> str:
    normalized_base = base_url.rstrip("/")
    return f"{normalized_base}/{dataset_name}/results/results.json"


def discover_datasets(datasets_root: Path) -> list[str]:
    if not datasets_root.exists():
        return []
    names = []
    for child in sorted(datasets_root.iterdir()):
        if child.is_dir() and dataset_results_path(datasets_root, child.name).exists():
            names.append(child.name)
    return names


def normalize_github_url(source: str) -> str:
    parsed = urlparse(source)
    if parsed.scheme not in {"http", "https"} or parsed.netloc != "github.com":
        return source

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 5 and parts[2] == "blob":
        owner, repo, _, branch = parts[:4]
        rest = "/".join(parts[4:])
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{rest}"
    return source


def load_results_from_source(source: str, fallback_path: Path | None = None) -> list[dict[str, Any]]:
    normalized_source = source.strip()
    if not normalized_source:
        if fallback_path is None:
            raise ValueError("results source is empty")
        return load_results(fallback_path)

    parsed = urlparse(normalized_source)
    if parsed.scheme in {"http", "https"}:
        url = normalize_github_url(normalized_source)
        try:
            with urlopen(url) as response:
                raw = json.load(response)
        except Exception:
            if fallback_path is None or not fallback_path.exists():
                raise
            raw = json.loads(fallback_path.read_text(encoding="utf-8"))
    else:
        raw = json.loads(Path(normalized_source).expanduser().read_text(encoding="utf-8"))

    return parse_results_payload(raw)


def load_results_for_dataset(dataset_name: str, datasets_root: Path, github_logs_base_url: str) -> tuple[list[dict[str, Any]], str, Path]:
    fallback_path = dataset_results_path(datasets_root, dataset_name)
    source_url = dataset_results_url(github_logs_base_url, dataset_name)
    if fallback_path.exists():
        return load_results(fallback_path), str(fallback_path), fallback_path
    return load_results_from_source(source_url, fallback_path), source_url, fallback_path


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_app_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return DEFAULT_APP_CONFIG
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("streamlit app config root must be a JSON object")
    return deep_merge(DEFAULT_APP_CONFIG, loaded)
