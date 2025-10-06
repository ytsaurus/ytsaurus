LIBRARY()

SRCS(
    events.cpp
    internal.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/keyvalue
    contrib/ydb/core/protos
    contrib/ydb/core/persqueue/public/counters
    contrib/ydb/core/tablet
    contrib/ydb/public/api/protos
    contrib/ydb/library/persqueue/topic_parser
)

END()
