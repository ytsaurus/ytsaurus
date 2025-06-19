LIBRARY()

SRCS(
    GLOBAL broken_dedup.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/normalizer/abstract
)

END()
