from __future__ import annotations

import unittest

import pandas as pd

from battery_results_app import (
    build_dataset_comparison_table,
    build_flags,
    compare_subsystems_to_fleet,
    subsystem_dataframe,
    summarize_dataset,
)


class ComparisonAndFlagsTests(unittest.TestCase):
    def test_summarize_dataset_and_comparison_table(self) -> None:
        primary = pd.DataFrame(
            [
                {
                    "rating": "Critical",
                    "battery_condition": "Poor",
                    "dominant_cause": "load",
                    "brownout_events": 2,
                    "min_enabled_voltage_v": 8.0,
                    "current_p99_a": 180.0,
                    "internal_resistance_mohm": 20.0,
                    "enabled_duration_s": 12.0,
                },
                {
                    "rating": "Good",
                    "battery_condition": "Good",
                    "dominant_cause": "mixed",
                    "brownout_events": 0,
                    "min_enabled_voltage_v": 10.0,
                    "current_p99_a": 120.0,
                    "internal_resistance_mohm": 15.0,
                    "enabled_duration_s": 10.0,
                },
            ]
        )
        secondary = pd.DataFrame(
            [
                {
                    "rating": "Good",
                    "battery_condition": "Good",
                    "dominant_cause": "mixed",
                    "brownout_events": 0,
                    "min_enabled_voltage_v": 10.5,
                    "current_p99_a": 110.0,
                    "internal_resistance_mohm": 14.0,
                    "enabled_duration_s": 9.0,
                }
            ]
        )

        summary = summarize_dataset(primary)
        self.assertEqual(summary["log_count"], 2)
        self.assertEqual(summary["critical_count"], 1)
        self.assertEqual(summary["brownout_log_count"], 1)

        table = build_dataset_comparison_table(primary, secondary, "A", "B")
        self.assertIn("metric", table.columns)
        self.assertIn("A", table.columns)
        self.assertIn("B", table.columns)
        self.assertEqual(table.iloc[0]["metric"], "Logs")

    def test_compare_subsystems_to_fleet_computes_peer_deltas(self) -> None:
        selected = pd.DataFrame(
            [
                {
                    "subsystem": "Drive/Module0/turn",
                    "family": "drive_turn",
                    "current_type": "stator",
                    "source_entry": "/Drive/Module0/turnCurrent",
                    "peak_a": 80.0,
                    "p90_a": 60.0,
                    "p95_a": 70.0,
                    "p99_a": 75.0,
                    "p50_a": 20.0,
                }
            ]
        )
        peer_a = selected.assign(peak_a=60.0, p90_a=40.0, p95_a=45.0, p99_a=50.0)
        peer_b = selected.assign(peak_a=70.0, p90_a=50.0, p95_a=55.0, p99_a=60.0)

        comparison = compare_subsystems_to_fleet(
            "q1",
            selected,
            {"q1": selected, "q2": peer_a, "q3": peer_b},
        )

        self.assertEqual(len(comparison), 1)
        row = comparison.iloc[0]
        self.assertEqual(row["subsystem"], "Drive/Module0/turn")
        self.assertAlmostEqual(row["p99_delta_vs_peer_median"], 20.0)
        self.assertGreaterEqual(row["p99_percentile"], 100.0)

    def test_build_flags_mentions_brownout_and_drive_spikes(self) -> None:
        all_logs = pd.DataFrame(
            [
                {"internal_resistance_mohm": 12.0},
                {"internal_resistance_mohm": 15.0},
                {"internal_resistance_mohm": 18.0},
            ]
        )
        selected = pd.Series(
            {
                "min_enabled_voltage_v": 7.8,
                "p05_enabled_voltage_v": 9.2,
                "peak_current_a": 260.0,
                "current_p90_a": 90.0,
                "current_p99_a": 120.0,
                "current_p50_a": 40.0,
                "internal_resistance_mohm": 18.0,
                "time_below_9v_s": 0.8,
                "brownout_events": 2,
            }
        )
        subsystem_df = subsystem_dataframe(
            {
                "Drive/Module0/turn": {"peak_a": 80.0, "p99_a": 70.0, "p95_a": 60.0, "p90_a": 50.0, "p50_a": 10.0},
                "Drive/Module1/turn": {"peak_a": 75.0, "p99_a": 65.0, "p95_a": 55.0, "p90_a": 45.0, "p50_a": 10.0},
                "Drive/Module0/drive": {"peak_a": 90.0, "p99_a": 75.0, "p95_a": 65.0, "p90_a": 55.0, "p50_a": 20.0},
            }
        )
        subsystem_comparison = pd.DataFrame(
            [
                {
                    "subsystem": "Drive/Module0/turn",
                    "p99_delta_vs_peer_median": 8.0,
                    "p95_delta_vs_peer_median": 4.0,
                    "peak_delta_vs_peer_median": 10.0,
                }
            ]
        )
        app_config = {"display_names": {}, "subsystem_groups": []}

        flags = build_flags(selected, all_logs, subsystem_df, subsystem_comparison, app_config)

        text = " ".join(flags)
        self.assertIn("Brownout behavior dominated", text)
        self.assertIn("Top instantaneous contributors", text)
        self.assertIn("drivetrain", text)


if __name__ == "__main__":
    unittest.main()
