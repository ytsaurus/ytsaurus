LIBRARY()

SRCS(
    abstract.cpp
)

PEERDIR(
    contrib/ydb/core/tx/tiering/tier
    contrib/ydb/core/tx/columnshard/blobs_action/protos
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/public/sdk/cpp/src/client/resources
    yql/essentials/core/expr_nodes
)

END()
