from __future__ import annotations

import unittest

from battery_health import DEFAULT_CONFIG, determine_dominant_cause, rate_battery_condition, rate_load_assessment


class ScoringTests(unittest.TestCase):
    def test_rate_battery_condition_uses_resistance_thresholds(self) -> None:
        self.assertEqual(
            rate_battery_condition(0.040, 12.8, DEFAULT_CONFIG),
            ("Poor", "Internal resistance is high, which points to a weak battery."),
        )
        self.assertEqual(
            rate_battery_condition(0.020, 12.0, DEFAULT_CONFIG),
            ("Good", "Internal resistance looks healthy."),
        )

    def test_rate_battery_condition_falls_back_to_resting_voltage(self) -> None:
        self.assertEqual(
            rate_battery_condition(None, 12.0, DEFAULT_CONFIG),
            ("Poor", "Resting voltage was low before or between load events."),
        )
        self.assertEqual(
            rate_battery_condition(None, 12.7, DEFAULT_CONFIG),
            ("Excellent", "Resting voltage looked strong."),
        )

    def test_rate_load_assessment_buckets_current_severity(self) -> None:
        subsystem_stats = {"drive": {"p99_a": 70.0}}
        self.assertEqual(
            rate_load_assessment({"p99_a": 140.0}, subsystem_stats, 0.5, 0),
            ("Moderate", "The robot saw moderate current demand."),
        )
        self.assertEqual(
            rate_load_assessment({"p99_a": 210.0}, subsystem_stats, 0.5, 0),
            ("Extreme", "The robot was under unusually heavy current demand."),
        )

    def test_determine_dominant_cause_prefers_load_for_strong_battery_high_load(self) -> None:
        self.assertEqual(determine_dominant_cause("Critical", "Excellent", "Extreme"), "load")
        self.assertEqual(determine_dominant_cause("Poor", "Poor", "Normal"), "battery")


if __name__ == "__main__":
    unittest.main()
