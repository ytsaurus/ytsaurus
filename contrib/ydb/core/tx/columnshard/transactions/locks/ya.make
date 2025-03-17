LIBRARY()

SRCS(
    dependencies.cpp
    interaction.cpp
    abstract.cpp
    GLOBAL read_start.cpp
    GLOBAL read_finished.cpp
    GLOBAL write.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/transactions/protos
    contrib/ydb/core/tx/columnshard/engines/predicate
    contrib/ydb/core/tx/columnshard/blobs_action/events
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
)

END()
