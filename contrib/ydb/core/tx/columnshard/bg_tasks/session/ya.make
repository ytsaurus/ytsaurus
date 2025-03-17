LIBRARY()

SRCS(
    session.cpp
    storage.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/bg_tasks/abstract
    contrib/ydb/core/tx/columnshard/bg_tasks/protos
)

END()
