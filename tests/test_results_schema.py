from __future__ import annotations

import unittest
from pathlib import Path

from results_schema import RESULTS_SCHEMA_VERSION, build_results_payload, infer_dataset_name, parse_results_payload


class ResultsSchemaTests(unittest.TestCase):
    def test_parse_results_payload_accepts_legacy_list(self) -> None:
        records = [{"log_path": "a.wpilog", "rating": "Good"}]
        self.assertEqual(parse_results_payload(records), records)

    def test_parse_results_payload_accepts_wrapped_object(self) -> None:
        wrapped = {
            "schema_version": RESULTS_SCHEMA_VERSION,
            "dataset_metadata": {"dataset_name": "practice"},
            "records": [{"log_path": "a.wpilog", "rating": "Good"}],
        }
        self.assertEqual(parse_results_payload(wrapped), wrapped["records"])

    def test_infer_dataset_name_prefers_standard_output_layout(self) -> None:
        log_paths = [Path("/tmp/other_dir/match1.wpilog")]
        output_path = Path("/tmp/logs/practice_04_05_26/results/results.json")
        self.assertEqual(infer_dataset_name(log_paths, output_path), "practice_04_05_26")

    def test_build_results_payload_includes_schema_metadata(self) -> None:
        records = [{"log_path": "/tmp/logs/practice/match1.wpilog", "rating": "Good"}]
        payload = build_results_payload(records, [Path(records[0]["log_path"])])

        self.assertEqual(payload["schema_version"], RESULTS_SCHEMA_VERSION)
        self.assertEqual(payload["dataset_metadata"]["dataset_name"], "practice")
        self.assertEqual(payload["dataset_metadata"]["record_count"], 1)
        self.assertTrue(payload["dataset_metadata"]["generated_at"].endswith("Z"))
        self.assertEqual(payload["records"], records)


if __name__ == "__main__":
    unittest.main()
