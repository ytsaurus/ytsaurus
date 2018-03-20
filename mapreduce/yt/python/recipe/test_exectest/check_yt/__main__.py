import sys

import yt.wrapper

if __name__ == "__main__":
    port_file = sys.argv[1]
    with open(port_file) as f:
        port = f.read()

    yt.wrapper.config["proxy"]["url"] = "localhost:" + port
    yt.wrapper.config["proxy"]["enable_proxy_discovery"] = False

    assert not yt.wrapper.exists("//tmp/table")

    yt.wrapper.create_table("//tmp/table")

    assert yt.wrapper.exists("//tmp/table")
