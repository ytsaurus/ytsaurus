LIBRARY()

SRCS(
    counters.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/counters/common
    contrib/ydb/core/tx/columnshard/engines/portions
)

END()
