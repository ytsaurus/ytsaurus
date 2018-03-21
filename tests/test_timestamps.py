import pytest

@pytest.mark.usefixtures("yp_env")
class TestTimestamps(object):
    def test_monotonicity(self, yp_env):
        ts1 = yp_env.yp_client.generate_timestamp()
        ts2 = yp_env.yp_client.generate_timestamp()
        assert ts1 < ts2
