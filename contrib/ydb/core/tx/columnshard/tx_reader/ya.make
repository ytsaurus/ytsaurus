LIBRARY()

SRCS(
    abstract.cpp
    composite.cpp
    lambda.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/counters
)

END()
