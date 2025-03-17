LIBRARY()

SRCS(
    selector.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/library/conclusion
    contrib/ydb/core/tx/datashard
    contrib/ydb/core/protos
)

END()
