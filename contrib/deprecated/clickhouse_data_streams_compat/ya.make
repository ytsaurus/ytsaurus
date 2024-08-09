LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(21.10.6.2)

ORIGINAL_SOURCE(https://github.com/ClickHouse/ClickHouse/archive/59536b3bd11f81eca0cff44d46f3a37457bd8c55.tar.gz)

PEERDIR(
    contrib/clickhouse/src
)

ADDINCL(
    GLOBAL contrib/deprecated/clickhouse_data_streams_compat
)

SRCS(
    # Used in many projects (chyt, metrika, ...)
    DataStreams/IBlockInputStream.cpp
    # Used in metrika
    DataStreams/RemoteBlockOutputStream.cpp
    DataStreams/RemoteBlockInputStream.cpp
    DataStreams/NativeBlockOutputStream.cpp
    # Used in yql/udfs/common/clickhouse
    DataStreams/NativeBlockInputStream.cpp
    # Used in many projects (chyt, metrika, ...)
    Processors/Sources/SourceFromInputStream.cpp
    Processors/Sources/SinkToOutputStream.cpp
)

END()
