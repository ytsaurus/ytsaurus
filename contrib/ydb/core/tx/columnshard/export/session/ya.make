LIBRARY()

SRCS(
    GLOBAL session.cpp
    cursor.cpp
    GLOBAL task.cpp
    GLOBAL control.cpp
)

PEERDIR(
    contrib/ydb/core/grpc_services/cancelation/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/bg_tasks
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/core/tx/columnshard/blobs_action/protos
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    yql/essentials/core/expr_nodes
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()
