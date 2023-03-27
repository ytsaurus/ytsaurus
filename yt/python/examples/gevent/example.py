# -*- coding: utf-8 -*-

import gevent
import os

import yt.wrapper

from gevent import monkey


def get(proxy, path):
    client = yt.wrapper.YtClient(proxy=proxy)
    return client.get(path)


def main():
    monkey.patch_all()

    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    gl = gevent.spawn(get, cluster, "//@type")
    gl.join()
    assert gl.value == "map_node"


if __name__ == "__main__":
    main()
