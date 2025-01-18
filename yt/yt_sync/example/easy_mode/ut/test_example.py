from yatest import common as ycommon


def test_example():
    bin_path = ycommon.binary_path("yt/yt_sync/example/easy_mode/yt_sync")
    env = {"YT_TOKEN": "token"}
    ycommon.execute([bin_path, "-S", "testing", "--scenario", "dump_spec"], env=env, check_exit_code=True)
