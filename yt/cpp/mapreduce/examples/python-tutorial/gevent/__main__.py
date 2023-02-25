# -*- coding: utf-8 -*-

import yt.wrapper

from gevent import monkey
import gevent


def get(proxy, path):
    client = yt.wrapper.YtClient(proxy=proxy)
    return client.get(path)


def main():
    monkey.patch_all()

    gl = gevent.spawn(get, "hume", "//@type")
    gl.join()
    assert gl.value == "map_node"


if __name__ == "__main__":
    main()
