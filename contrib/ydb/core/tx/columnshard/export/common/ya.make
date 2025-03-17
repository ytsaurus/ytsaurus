LIBRARY()

SRCS(
    identifier.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/library/conclusion
    contrib/ydb/core/protos
)

END()
