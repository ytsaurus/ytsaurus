LIBRARY()

SRCS(
    GLOBAL gc_counters.cpp
    GLOBAL broken_txs.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/normalizer/abstract
)

END()
