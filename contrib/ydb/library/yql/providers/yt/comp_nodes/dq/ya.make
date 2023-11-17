LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/providers/yt/comp_nodes
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/core/formats/arrow
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/common
    library/cpp/yson/node
    yt/yt/core
    contrib/ydb/library/yql/public/udf/arrow
    contrib/libs/apache/arrow
    contrib/libs/flatbuffers
)

ADDINCL(
    contrib/libs/flatbuffers/include
)

IF(LINUX)
    PEERDIR(
        yt/yt/client
        yt/yt/client/arrow
        contrib/ydb/library/yql/providers/yt/lib/yt_rpc_helpers
    )

    SRCS(
        stream_decoder.cpp
        dq_yt_rpc_reader.cpp
        dq_yt_rpc_helpers.cpp
        dq_yt_block_reader.cpp
    )
    CFLAGS(
        -Wno-unused-parameter
    )
ENDIF()

SRCS(
    dq_yt_reader.cpp
    dq_yt_factory.cpp
    dq_yt_writer.cpp
)

YQL_LAST_ABI_VERSION()


END()
