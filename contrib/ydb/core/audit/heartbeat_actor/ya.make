LIBRARY()

SRCS(
    heartbeat_actor.cpp
)

PEERDIR(
    contrib/ydb/core/audit
    contrib/ydb/library/actors/core
)

END()
