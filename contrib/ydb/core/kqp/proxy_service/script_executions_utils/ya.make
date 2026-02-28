LIBRARY()

SRCS(
    kqp_script_execution_compression.cpp
    kqp_script_execution_retries.cpp
)

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/protobuf/interop
    contrib/ydb/core/protos
    contrib/ydb/core/tx/datashard
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/api/protos
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
