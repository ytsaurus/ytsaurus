LIBRARY()

SRCS(
    context.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
)

END()
