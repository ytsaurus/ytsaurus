LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
)

ADDINCL(
    contrib/ydb/library/arrow_clickhouse/base
    contrib/ydb/library/arrow_clickhouse
)

SRCS(
    Allocator.cpp
    PODArray.cpp
)

END()
