import pytest
import yatest.common as yatest_common

from mapreduce.yt.python.yt_stuff import YtConfig, YtStuff


def test_local_cypress_dir():
    config = YtConfig(
        local_cypress_dir=yatest_common.source_path("mapreduce/yt/python/test/cypress_dir")
    )

    stuff = YtStuff(config)
    stuff.start_local_yt()
    stuff.stop_local_yt()


def test_invalid_local_cypress_dir():
    config = YtConfig(
        local_cypress_dir=yatest_common.source_path("mapreduce/yt/python/test/missing_cypress_dir")
    )

    with pytest.raises(Exception):
        stuff = YtStuff(config)
        stuff.start_local_yt()
