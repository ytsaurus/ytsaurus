LIBRARY()

SRCS(
    abstract.cpp
    compaction_info.cpp
    settings.cpp
    remove_portions.cpp
    move_portions.cpp
    changes.cpp
)

PEERDIR(
    contrib/ydb/library/signals
    contrib/ydb/core/tx/columnshard/engines/changes/counters
    contrib/ydb/core/tablet_flat
    yql/essentials/core/expr_nodes
    contrib/ydb/core/tx/columnshard/blobs_action
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
