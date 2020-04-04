#!/usr/bin/python

import yt.wrapper as yt
import sys

if __name__ == "__main__":
    proxies = yt.get("//sys/rpc_proxies")
    for addr in proxies.iterkeys():
        if "alive" not in proxies[addr]:
            print "Removing ", addr
            yt.remove("//sys/rpc_proxies/" + addr, recursive=True)    