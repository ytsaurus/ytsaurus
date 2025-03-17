LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/sys_view/abstract
)

SRCS(
    GLOBAL optimizer.cpp
)

END()

