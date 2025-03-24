LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/core/protos
    contrib/ydb/core/tx/schemeshard/olap/common
)

END()
