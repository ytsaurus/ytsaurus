LIBRARY()

SRCS(
    global.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/bg_tasks/abstract
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/protos
)

END()
