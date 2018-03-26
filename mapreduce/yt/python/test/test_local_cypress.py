import pytest
import yatest.common as yatest_common

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig, yt_stuff

@pytest.fixture(scope="module")
def yt_config(request):
    return YtConfig(
        local_cypress_dir=yatest_common.source_path("mapreduce/yt/python/test/cypress_dir")
    )

def test_invalid_local_cypress_dir(yt_stuff):
    yt_wrapper = yt_stuff.get_yt_wrapper()
    assert yt_wrapper.exists("//my_table")


