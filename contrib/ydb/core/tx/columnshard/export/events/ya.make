LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/tx/columnshard/export/common
    contrib/ydb/core/tx/columnshard/export/session
)

END()
