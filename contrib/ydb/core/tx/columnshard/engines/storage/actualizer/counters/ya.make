LIBRARY()

SRCS(
    counters.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/counters/common
)

END()
