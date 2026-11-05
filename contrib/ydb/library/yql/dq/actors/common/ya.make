YQL_LIBRARY()

SRCS(
    retry_queue.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yverify_stream
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
