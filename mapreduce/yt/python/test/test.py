import sys
import yatest.common

from mapreduce.yt.python.yt_stuff import YtStuff
from mapreduce.yt.python.yt_stuff import yt_stuff


def test_start_stop():
    yt = YtStuff()
    yt.start_local_yt()
    yt.stop_local_yt()

def test_fixture(yt_stuff):
    pass

def test_mapreduce_yt(yt_stuff):
    yt_server = yt_stuff.get_server()
    mapreduce_yt = yt_stuff.get_mapreduce_yt()
    yatest.common.execute(
        [sys.executable, mapreduce_yt, "-server", yt_server, "-createtable", "//table"],
        cwd=yt_stuff.python_dir
    )
