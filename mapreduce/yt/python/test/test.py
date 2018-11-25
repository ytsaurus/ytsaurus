from mapreduce.yt.python.yt_stuff import YtStuff

from os import environ

environ["YT_STUFF_MAX_START_RETRIES"] = "2"


def test_start_stop():
    yt = YtStuff()
    yt.start_local_yt()
    yt.stop_local_yt()


def test_fixture(yt_stuff):
    pass


def test_yt_wrapper(yt_stuff):
    yt_client = yt_stuff.get_yt_client()
    assert not yt_client.exists("//hello/world/path")


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
