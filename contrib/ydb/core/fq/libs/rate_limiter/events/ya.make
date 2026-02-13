LIBRARY()

SRCS(
    control_plane_events.cpp
    data_plane.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/fq/libs/events
)

END()
