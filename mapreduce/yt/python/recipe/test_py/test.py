import os
import yt.wrapper


def test():
    yt_proxy = os.environ["YT_PROXY"]

    yt.wrapper.config["proxy"]["url"] = yt_proxy
    yt.wrapper.config["proxy"]["enable_proxy_discovery"] = False

    assert not yt.wrapper.exists("//tmp/table")

    yt.wrapper.create_table("//tmp/table")

    assert yt.wrapper.exists("//tmp/table")
    assert "YT_RPC_PROXY" in os.environ
