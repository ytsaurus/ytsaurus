#!/usr/bin/env python3
import yt.wrapper as yt

config = yt.get("//sys/clickhouse/config")
cluster_connection = yt.get("//sys/@cluster_connection")
config["cluster_connection"] = cluster_connection

print(yt.yson.dumps(config, yson_format="pretty").decode("utf-8"))
