LIBRARY()

SRCS(
    overload_manager_actor.cpp
    overload_manager_service.cpp
    overload_subscribers.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/data_events
)

END()
