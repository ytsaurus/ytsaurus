LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL bitset.cpp
    GLOBAL string.cpp
)

PEERDIR(
    contrib/ydb/library/conclusion
    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
)

END()
