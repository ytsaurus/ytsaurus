LIBRARY()

SRCS(
    common.h
    events.h
)

PEERDIR(
    util
    contrib/ydb/library/actors/core
    contrib/ydb/library/query_actor
    yql/essentials/core/minsketch
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
)

END()

RECURSE(
    aggregator
    database
    service
    ut_common
)

RECURSE_FOR_TESTS(
    aggregator/ut
    database/ut
    service/ut
)
