LIBRARY()

SRCS(
    utils.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/library/yverify_stream
    yql/essentials/sql/v1
)

YQL_LAST_ABI_VERSION()

END()
