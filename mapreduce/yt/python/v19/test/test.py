import sys

import yatest.common

from mapreduce.yt.python.v19.yt_stuff import YtStuff
from mapreduce.yt.python.v19.yt_stuff import yt_stuff

from os import environ
environ["YT_STUFF_MAX_START_RETRIES"] = "2"

def test_start_stop():
    yt = YtStuff()
    yt.start_local_yt()
    yt.stop_local_yt()

def test_fixture(yt_stuff):
    pass

def test_mapreduce_yt(yt_stuff):
    yt_server = yt_stuff.get_server()
    yt_stuff.run_mapreduce_yt(["-server", yt_server, "-createtable", "//table1"])
    yt_stuff.run_mapreduce_yt(
        ["-server", yt_server, "-createtable", "//table2"],
        timeout=100,
        check_exit_code=True
    )

def test_yt_wrapper(yt_stuff):
    yt_wrapper = yt_stuff.get_yt_wrapper()
    assert not yt_wrapper.exists("//hello/world/path")

def test_scheme(yt_stuff):
    wrapper = yt_stuff.yt_wrapper

    wrapper.create_table(
        "//test/schema_test",
        recursive=True
    )
    wrapper.write_table(
        "//test/schema_test",
        """{"a"=1}""",
        format=wrapper.YsonFormat(),
        raw=True
    )
