from __future__ import annotations

import unittest

from battery_health import build_trace_points
from battery_results_app import extract_traces_by_log, trace_dataframe


class TraceExportTests(unittest.TestCase):
    def test_build_trace_points_downsamples_and_preserves_endpoints(self) -> None:
        voltage_series = [(index * 100_000, 12.5 - index * 0.1) for index in range(10)]
        current_series = [(index * 100_000, 20.0 + index) for index in range(10)]
        enabled_series = [(0, False), (200_000, True)]
        autonomous_series = [(0, True), (500_000, False)]
        browned_out_series = [(0, False), (700_000, True)]

        trace = build_trace_points(
            voltage_series,
            current_series,
            enabled_series,
            autonomous_series,
            browned_out_series,
            max_points=4,
        )

        self.assertIsNotNone(trace)
        assert trace is not None
        self.assertEqual(trace["sample_count"], 4)
        self.assertEqual(trace["points"][0]["time_s"], 0.0)
        self.assertAlmostEqual(trace["points"][-1]["time_s"], 0.9)
        self.assertEqual(trace["points"][0]["enabled"], False)
        self.assertEqual(trace["points"][-1]["browned_out"], True)

    def test_extract_traces_and_trace_dataframe(self) -> None:
        records = [
            {
                "log_path": "/tmp/log1.wpilog",
                "trace": {
                    "duration_s": 1.0,
                    "sample_count": 2,
                    "points": [
                        {"time_s": 0.0, "voltage_v": 12.0, "pack_current_a": 10.0, "enabled": False, "autonomous": True, "browned_out": False},
                        {"time_s": 1.0, "voltage_v": 11.0, "pack_current_a": 40.0, "enabled": True, "autonomous": False, "browned_out": True},
                    ],
                },
            }
        ]

        traces_by_log = extract_traces_by_log(records)
        self.assertIn("/tmp/log1.wpilog", traces_by_log)

        df = trace_dataframe(traces_by_log["/tmp/log1.wpilog"])
        self.assertEqual(list(df.columns), ["time_s", "voltage_v", "pack_current_a", "enabled", "autonomous", "browned_out"])
        self.assertEqual(df["enabled"].tolist(), [0, 1])
        self.assertEqual(df["browned_out"].tolist(), [0, 1])


if __name__ == "__main__":
    unittest.main()
