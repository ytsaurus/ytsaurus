LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/control_plane_storage/events
    contrib/ydb/core/fq/libs/events
    yql/essentials/public/issue/protos
    contrib/ydb/public/api/protos
)

END()
