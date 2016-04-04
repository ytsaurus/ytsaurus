import sys
import pytest
import yatest.common

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig
from mapreduce.yt.python.yt_stuff import yt_stuff

TEST_ID = "test_with_config"

@pytest.fixture(scope="module")
def yt_config(request):
    return YtConfig(yt_id=TEST_ID)

def test_fixture(yt_stuff):
    assert yt_stuff.config.yt_id == TEST_ID

