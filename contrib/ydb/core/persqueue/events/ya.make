LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/keyvalue
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/public/api/protos
    contrib/ydb/library/persqueue/topic_parser
)

END()
