LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/fq/libs/quota_manager/events
)

END()
