from mapreduce.yt.python.yt_stuff import yt_stuff


def test_suspend(yt_stuff):
    wrapper = yt_stuff.get_yt_wrapper()
    path = "//test/suspend_test"

    wrapper.create_table(path, recursive=True)
    assert wrapper.exists(path)

    yt_stuff.suspend_local_yt()
    yt_stuff.start_local_yt()

    assert wrapper.exists(path)

