LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/tx/schemeshard/olap/table
    contrib/ydb/core/tx/schemeshard/olap/layout
)

END()
