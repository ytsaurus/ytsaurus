LIBRARY()

SRCS(
    GLOBAL selector.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/session/selector/abstract
    contrib/ydb/core/protos
    contrib/ydb/library/yql/dq/actors/protos
)

END()
