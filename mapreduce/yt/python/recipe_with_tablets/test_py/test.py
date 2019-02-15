import os
import yt.wrapper


def test_create_table():
    yt_proxy = os.environ["YT_PROXY"]

    yt.wrapper.config["proxy"]["url"] = yt_proxy
    yt.wrapper.config["proxy"]["enable_proxy_discovery"] = False

    assert not yt.wrapper.exists("//tmp/table")

    yt.wrapper.create("table", "//tmp/table")

    assert yt.wrapper.exists("//tmp/table")


def test_dynamic_table():
    yt_proxy = os.environ["YT_PROXY"]

    yt.wrapper.config["proxy"]["url"] = yt_proxy
    yt.wrapper.config["proxy"]["enable_proxy_discovery"] = False

    yt.wrapper.create("table", "//tmp/dynamic_table", attributes={
        "dynamic": True,
        "schema": [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ],
    })
    yt.wrapper.mount_table("//tmp/dynamic_table", sync=True)
    yt.wrapper.insert_rows("//tmp/dynamic_table", [{"key": "answer", "value": "42"}])
    assert list(yt.wrapper.select_rows("* from [//tmp/dynamic_table]")) == [{"key": "answer", "value": "42"}]
