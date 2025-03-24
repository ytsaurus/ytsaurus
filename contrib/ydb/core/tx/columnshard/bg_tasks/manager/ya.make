LIBRARY()

SRCS(
    manager.cpp
    actor.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/bg_tasks/abstract
    contrib/ydb/core/tx/columnshard/bg_tasks/protos
    contrib/ydb/core/tablet_flat
)

END()
