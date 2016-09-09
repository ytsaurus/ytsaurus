import sys
import pytest
import yatest.common

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig
from mapreduce.yt.python.yt_stuff import yt_stuff

TEST_ID = "test_with_config"

@pytest.fixture(scope="module")
def yt_config(request):
    return YtConfig(
        yt_id=TEST_ID,
        wait_tablet_cell_initialization=True,
        operations_memory_limit=4 * 1024 * 1024 * 1024,
    )


def test_fixture(yt_stuff):
    assert yt_stuff.config.yt_id == TEST_ID

