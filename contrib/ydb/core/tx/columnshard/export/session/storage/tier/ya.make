LIBRARY()

SRCS(
    GLOBAL storage.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/session/selector/abstract
)

END()
