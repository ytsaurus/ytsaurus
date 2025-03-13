LIBRARY()

SRCS(
    export_actor.cpp
    write.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/engines/writer
    contrib/ydb/core/tx/columnshard/export/events
    contrib/ydb/core/kqp/compute_actor
)

END()
