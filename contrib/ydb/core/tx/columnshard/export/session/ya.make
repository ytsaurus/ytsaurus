LIBRARY()

SRCS(
    GLOBAL session.cpp
    cursor.cpp
    GLOBAL task.cpp
    GLOBAL control.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/session/selector
    contrib/ydb/core/tx/columnshard/export/session/storage
    contrib/ydb/core/tx/columnshard/bg_tasks
    contrib/ydb/core/scheme
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()
