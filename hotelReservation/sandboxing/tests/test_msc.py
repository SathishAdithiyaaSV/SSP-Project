import unittest


def detect_msc(steps, success_threshold, p90_threshold):
    best = 0
    for step in steps:
        if step["success_rate"] >= success_threshold and step["p90_latency_ms"] <= p90_threshold:
            best = step["rps"]
        else:
            break
    return best


class MSCTest(unittest.TestCase):
    def test_detects_last_passing_step(self) -> None:
        steps = [
            {"rps": 1, "success_rate": 1.0, "p90_latency_ms": 50},
            {"rps": 5, "success_rate": 0.99, "p90_latency_ms": 100},
            {"rps": 10, "success_rate": 0.97, "p90_latency_ms": 200},
        ]
        self.assertEqual(detect_msc(steps, 0.98, 3000), 5)


if __name__ == "__main__":
    unittest.main()
