from os import environ
environ["YT_STUFF_MAX_START_RETRIES"] = "2"


def test_suspend(yt_stuff):
    client = yt_stuff.get_yt_client()
    path = "//test/suspend_test"

    client.create_table(path, recursive=True)
    assert client.exists(path)

    yt_stuff.suspend_local_yt()
    yt_stuff.start_local_yt()

    assert client.exists(path)
