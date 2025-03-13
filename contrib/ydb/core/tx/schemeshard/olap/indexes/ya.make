LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/core/tx/columnshard/engines/scheme/indexes/abstract
)

END()
