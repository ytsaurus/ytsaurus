import pytest
import yatest.common

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig, yt_stuff

from os import environ
environ["YT_STUFF_MAX_START_RETRIES"] = "2"

TEST_ID = "test_with_config"

@pytest.fixture(scope="module")
def yt_config(request):
    return YtConfig(
        yt_id=TEST_ID,
        wait_tablet_cell_initialization=True,
    )

def test_fixture(yt_stuff):
    assert yt_stuff.config.yt_id == TEST_ID

