LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/proto
    yql/essentials/public/issue/protos
)

END()
