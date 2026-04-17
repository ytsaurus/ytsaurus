LIBRARY()

SRCS(
    GLOBAL session.cpp
    GLOBAL task.cpp
    GLOBAL control.cpp
    import_actor.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/compute_actor
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/backup/import/protos
    contrib/ydb/core/tx/columnshard/bg_tasks
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()

RECURSE_FOR_TESTS(
    ut
)
