LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

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
