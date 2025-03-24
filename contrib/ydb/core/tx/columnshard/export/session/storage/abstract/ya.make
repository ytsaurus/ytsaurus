LIBRARY()

SRCS(
    storage.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/session/storage/s3
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/library/conclusion
    contrib/ydb/core/protos
)

END()
