LIBRARY()

SRCS(
    actor2.cpp
    events.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat
    yql/essentials/core/expr_nodes
    contrib/ydb/library/actors/testlib/common
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/blobs_action/protos
)

END()
