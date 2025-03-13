LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/tiering/tier
)

END()
