LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/core/fq/libs/checkpointing_common
    contrib/ydb/core/fq/libs/control_plane_storage/events
    yql/essentials/public/issue
    contrib/ydb/public/api/protos
)

END()
