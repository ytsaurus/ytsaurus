from dataclasses import dataclass


class Telemetry:
    @dataclass
    class _Transport:
        # top-level method count (client.make_request)
        call_count: int = 0
        # top-level method fails
        fail_count: int = 0
        # retries count
        retries_count: int = 0
        # low-level requests count (rpc_driver.execute())
        requests_count: int = 0
        # time spent in low-level requests
        requests_seconds: float = 0.0

    def __init__(self):
        self.transport = Telemetry._Transport()

    def clear_all(self):
        self.transport = Telemetry._Transport()


_telemetry = Telemetry()
