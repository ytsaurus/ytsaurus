LIBRARY()

SRCS(
    GLOBAL wrapper.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/json
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/tiling
    contrib/ydb/library/intersection_tree
)

END()
