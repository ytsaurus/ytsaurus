LIBRARY()

SRCS(
    actor.cpp
    task.cpp
    events.cpp
    read_coordinator.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/core/tablet_flat
)

END()
