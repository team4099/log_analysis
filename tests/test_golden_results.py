from __future__ import annotations

import json
import unittest
from pathlib import Path

from battery_health import analyze_logs, load_config
from results_schema import parse_results_payload


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "battery_health_config.json"


class GoldenResultsTests(unittest.TestCase):
    def test_checked_in_results_match_current_analyzer_output(self) -> None:
        config, _ = load_config(CONFIG_PATH, REPO_ROOT)

        for results_path in sorted(REPO_ROOT.glob("logs/*/results/results.json")):
            dataset_dir = results_path.parent.parent
            log_paths = sorted(dataset_dir.glob("*.wpilog"))
            with self.subTest(dataset=dataset_dir.name):
                self.assertTrue(log_paths, f"Expected WPILOG files under {dataset_dir}")
                expected_records = parse_results_payload(json.loads(results_path.read_text(encoding="utf-8")))
                actual_records = [summary.as_dict() for summary in analyze_logs(log_paths, jobs=1, config=config)]
                self.assertEqual(actual_records, expected_records)


if __name__ == "__main__":
    unittest.main()
