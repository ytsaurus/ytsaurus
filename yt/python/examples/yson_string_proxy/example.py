# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper
import yt.yson


def mapper(rec):
    # Operations process works the same way as reading from tables.
    assert not yt.yson.is_unicode(rec["y"])
    assert yt.yson.get_bytes(rec["y"]) == b"\xFF"
    yield {"x": int(rec["x"]) + 1000}


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    path = "//tmp/{}-yson-string-proxy".format(getpass.getuser())
    client.create("map_node", path, ignore_existing=True)

    # Writing in a raw mode. A list of binary strings goes to input.
    client.write_table(path + "/table", [br'{x=1;y="\xFF"};'], raw=True, format=yt.wrapper.YsonFormat())

    # Reading in YSON format by default. Records are decoded automatically.
    # Strings that could not be decoded are converted to `YsonStringProxy`.
    # You compare and hash them as bytes.
    rows = list(client.read_table(path + "/table"))
    assert rows == [{"x": 1, "y": b"\xFF"}]

    # You can also check if a string was parsed from the specified encoding (UTF-8 by default).
    value = rows[0]["y"]
    assert yt.yson.get_bytes(value) == b"\xFF"
    assert not yt.yson.is_unicode(value)

    # Writing in YSON format. Unicode strings will be encoded using utf-8
    client.write_table(path + "/other", [{"y": 1}], format=yt.wrapper.YsonFormat())
    assert list(client.read_table(path + "/other")) == [{"y": 1}]

    # We want to use a byte key in a record with regular keys.
    client.write_table(
        path + "/mixed",
        [
            {
                "a": "a",
                "b": "b",
                yt.yson.make_byte_key(b"\xFF"): b"aaa\xFF",
            }
        ],
    )
    assert list(client.read_table(path + "/mixed")) == [{"a": "a", "b": "b", b"\xFF": b"aaa\xFF"}]

    # We want to work with pure binary strings in records. Than setting encoding=None.
    client.write_table(path + "/binary", [{b"x": b"aaa\xFF"}], format=yt.wrapper.YsonFormat(encoding=None))
    rows = list(client.read_table(path + "/binary", format=yt.wrapper.YsonFormat(encoding=None)))
    assert rows == [{b"x": b"aaa\xFF"}]

    # It works the same way in operations.
    client.run_map(mapper, path + "/table", path + "/output", format=yt.wrapper.YsonFormat())
    assert list(client.read_table(path + "/output")) == [{"x": 1001}]


if __name__ == "__main__":
    main()
