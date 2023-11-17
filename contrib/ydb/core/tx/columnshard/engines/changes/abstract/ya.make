LIBRARY()

SRCS(
    abstract.cpp
    compaction_info.cpp
    settings.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/counters/common
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/core/tx/columnshard/blobs_action
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
