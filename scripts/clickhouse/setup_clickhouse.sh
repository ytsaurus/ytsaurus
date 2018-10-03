#!/bin/bash

# TODO(max42): generalize this.

if [ $# -ne 1 ] ; then
    echo "The only argument should be path to the node config" >&2
    exit 2
fi

yt create map_node //sys/clickhouse

yt create map_node //sys/clickhouse/config_files

yt create file //sys/clickhouse/config_files/config.xml
cat /home/max42/yt_arc/source/yt/tests/integration/tests/test_clickhouse/config.xml | yt write-file //sys/clickhouse/config_files/config.xml

yt create file //sys/clickhouse/config_files/config.yson
python -c """
import yt.yson as yson
config = yson.loads(open('/home/max42/yt_arc/source/yt/tests/integration/tests/test_clickhouse/config.yson').read())
config['cluster_connection'] = yson.loads(open('$1').read())['cluster_connection']
print yson.dumps(config, yson_format='pretty')
""" | yt write-file //sys/clickhouse/config_files/config.yson

yt create map_node //sys/clickhouse/configuration

yt create document //sys/clickhouse/configuration/server
cat /home/max42/yt_arc/source/yt/tests/integration/tests/test_clickhouse/server.yson | yt set //sys/clickhouse/configuration/server

yt create document //sys/clickhouse/configuration/users
cat /home/max42/yt_arc/source/yt/tests/integration/tests/test_clickhouse/users.yson | yt set //sys/clickhouse/configuration/users

