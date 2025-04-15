LIBRARY()

SRCS(
    counters.cpp
)

PEERDIR(
    contrib/ydb/library/signals
    contrib/ydb/core/tx/columnshard/engines/portions
)

END()
