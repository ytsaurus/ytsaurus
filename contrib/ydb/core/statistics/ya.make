LIBRARY()

SRCS(
    events.h
    stat_service.h
    stat_service.cpp
)

PEERDIR(
    util
    library/cpp/actors/core
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
)

END()

RECURSE(
    aggregator
)

RECURSE_FOR_TESTS(
    ut
)
