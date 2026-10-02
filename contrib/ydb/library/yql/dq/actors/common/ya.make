YQL_LIBRARY()

SRCS(
    retry_queue.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/actors/protos
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
