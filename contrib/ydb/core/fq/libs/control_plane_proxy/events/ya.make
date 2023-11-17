LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    contrib/ydb/core/fq/libs/control_plane_storage/events
    contrib/ydb/core/fq/libs/quota_manager/events
)

END()
