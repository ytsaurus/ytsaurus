RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/restricted/cityhash-1.0.2

    contrib/ydb/library/arrow_clickhouse/Common
    contrib/ydb/library/arrow_clickhouse/Columns
    contrib/ydb/library/arrow_clickhouse/DataStreams
)

ADDINCL(
    GLOBAL contrib/ydb/library/arrow_clickhouse/base
    contrib/ydb/library/arrow_clickhouse
)

SRCS(
    AggregateFunctions/IAggregateFunction.cpp
    Aggregator.cpp

    # used in Common/Allocator
    base/common/mremap.cpp
)

END()

RECURSE(
    Columns
    Common
    DataStreams
)
