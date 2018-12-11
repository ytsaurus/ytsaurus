from mapreduce.yt.python.yt_stuff import YtStuff

from os import environ

import yt.wrapper

environ["YT_STUFF_MAX_START_RETRIES"] = "2"


def test_start_stop():
    yt_stuff = YtStuff()
    yt_stuff.start_local_yt()
    yt_stuff.stop_local_yt()


def test_fixture(yt_stuff):
    pass


def test_yt_client(yt_stuff):
    yt_client = yt_stuff.get_yt_client()
    assert not yt_client.exists("//hello/world/path")


def test_scheme(yt_stuff):
    client = yt_stuff.get_yt_client()

    client.create_table(
        "//test/schema_test",
        recursive=True
    )
    client.write_table(
        "//test/schema_test",
        """{"a"=1}""",
        format=yt.wrapper.YsonFormat(),
        raw=True
    )
