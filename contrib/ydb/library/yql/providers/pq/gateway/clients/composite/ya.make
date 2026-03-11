LIBRARY()

SRCS(
    yql_pq_composite_read_session.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/threading/future
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/signals
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/sdk/cpp/src/client/topic
)

END()
