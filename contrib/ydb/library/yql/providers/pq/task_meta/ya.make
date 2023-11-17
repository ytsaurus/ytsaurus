LIBRARY()

SRCS(
    task_meta.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/pq/proto
)

YQL_LAST_ABI_VERSION()

END()
