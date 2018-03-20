import yt.wrapper


def test():
    with open("yt_proxy_port.txt") as f:
        port = f.read()

    yt.wrapper.config["proxy"]["url"] = "localhost:" + port
    yt.wrapper.config["proxy"]["enable_proxy_discovery"] = False

    assert not yt.wrapper.exists("//tmp/table")

    yt.wrapper.create_table("//tmp/table")

    assert yt.wrapper.exists("//tmp/table")
