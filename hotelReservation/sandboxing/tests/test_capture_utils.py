import unittest

from hotelReservation.sandboxing.capture_utils import canonical_json, extract_request_corpus, normalize_capture


class CaptureUtilsTest(unittest.TestCase):
    def test_normalize_capture_deduplicates_method_request_pairs(self) -> None:
        payload = [
            {
                "method": "/geo.Geo/Nearby",
                "request": {"lat": 1.0, "lon": 2.0},
                "response": {"hotelIds": ["1"]},
                "code": "OK",
            },
            {
                "method": "/geo.Geo/Nearby",
                "request": {"lat": 1.0, "lon": 2.0},
                "response": {"hotelIds": ["1"]},
                "code": "OK",
            },
        ]
        normalized = normalize_capture(payload)
        self.assertEqual(len(normalized["fixtures"]), 1)

    def test_extract_request_corpus_deduplicates(self) -> None:
        payload = [
            {"request": {"lat": 1.0, "lon": 2.0}},
            {"request": {"lat": 1.0, "lon": 2.0}},
            {"request": {"lat": 3.0, "lon": 4.0}},
        ]
        corpus = extract_request_corpus(payload)
        self.assertEqual(len(corpus["requests"]), 2)

    def test_canonical_json_rounds_float_noise(self) -> None:
        baseline = {"lat": 38.0235, "lon": -122.095}
        float32_noise = {"lat": 38.02349853515625, "lon": -122.09500122070312}
        self.assertEqual(canonical_json(baseline), canonical_json(float32_noise))


if __name__ == "__main__":
    unittest.main()
