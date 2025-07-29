LIBRARY()

SRCS(
    abstract.cpp
    result.cpp
    limit.cpp
    aggr.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
)

END()
