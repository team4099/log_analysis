from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from battery_results_app import load_results, normalize_records
from results_schema import build_results_payload


class AppLoadingTests(unittest.TestCase):
    def test_load_results_accepts_wrapped_payload(self) -> None:
        records = [{"log_path": "/tmp/logs/practice/q1.wpilog", "rating": "Good"}]
        payload = build_results_payload(records, [Path(records[0]["log_path"])])

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "results.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertEqual(load_results(path), records)

    def test_load_results_accepts_legacy_list_payload(self) -> None:
        records = [{"log_path": "/tmp/logs/practice/q2.wpilog", "rating": "Fair"}]

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "results.json"
            path.write_text(json.dumps(records), encoding="utf-8")
            self.assertEqual(load_results(path), records)

    def test_normalize_records_derives_match_labels_from_log_path(self) -> None:
        records = [
            {
                "log_path": "/tmp/logs/practice/akit_26-03-30_q12.wpilog",
                "rating": "Good",
                "summary": "Looks healthy",
                "battery_condition": "Good",
                "battery_condition_summary": "Healthy battery",
                "load_assessment": "Normal",
                "load_assessment_summary": "Normal load",
                "dominant_cause": "mixed",
                "enabled_duration_s": 10.0,
                "resting_voltage_v": 12.7,
                "min_enabled_voltage_v": 11.0,
                "p05_enabled_voltage_v": 11.2,
                "brownout_events": 0,
                "time_below_9v_s": 0.0,
                "time_below_10v_s": 0.0,
                "peak_current_a": 100.0,
                "current_stats": {"p50_a": 40.0, "p90_a": 80.0, "p95_a": 90.0, "p99_a": 95.0},
                "internal_resistance_ohm": 0.02,
                "subsystem_current_stats": {},
                "phase_summaries": {},
                "notes": [],
            }
        ]

        df, subsystem_stats, phase_summaries = normalize_records(records)

        self.assertEqual(df.loc[0, "match"], "q12")
        self.assertEqual(df.loc[0, "current_p99_a"], 95.0)
        self.assertEqual(df.loc[0, "internal_resistance_mohm"], 20.0)
        self.assertEqual(subsystem_stats[records[0]["log_path"]], {})
        self.assertEqual(phase_summaries[records[0]["log_path"]], {})


if __name__ == "__main__":
    unittest.main()
