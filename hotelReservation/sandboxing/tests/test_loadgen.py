import unittest
from unittest.mock import Mock, patch

import grpc

from hotelReservation.sandboxing.loadgen import run_search_step


class _ReadyFuture:
    def result(self, timeout=None) -> None:
        return None


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__()
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class LoadgenTest(unittest.TestCase):
    @patch("hotelReservation.sandboxing.loadgen.grpc.channel_ready_future", return_value=_ReadyFuture())
    @patch("hotelReservation.sandboxing.loadgen.grpc.insecure_channel")
    @patch("hotelReservation.sandboxing.loadgen.load_modules")
    def test_warmup_not_counted_and_failures_are_reported(self, mock_load_modules, mock_channel_factory, _mock_ready) -> None:
        channel = Mock()
        mock_channel_factory.return_value = channel

        stub = Mock()
        calls = {"count": 0}

        def nearby(*_args, **_kwargs):
            calls["count"] += 1
            if calls["count"] == 3:
                raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "transient failure")
            return object()

        stub.Nearby.side_effect = nearby
        grpc_module = Mock(SearchStub=Mock(return_value=stub))
        proto_module = Mock(NearbyRequest=Mock(side_effect=lambda **payload: payload))
        mock_load_modules.return_value = (proto_module, grpc_module)

        result = run_search_step(
            repo_root=Mock(),
            target="127.0.0.1:8082",
            requests_corpus=[{"lat": 1.0, "lon": 2.0, "inDate": "2015-04-09", "outDate": "2015-04-10"}],
            rps=1,
            duration_seconds=2,
        )

        self.assertEqual(result.count, 2)
        self.assertEqual(result.failures, 1)
        self.assertAlmostEqual(result.success_rate, 0.5)
        self.assertEqual(result.error_summary, [{"error": "UNAVAILABLE: transient failure", "count": 1}])
        self.assertEqual(stub.Nearby.call_count, 3)
        channel.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
