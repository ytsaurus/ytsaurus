LIBRARY()

ADDINCL(
    contrib/libs/poco/Foundation/include
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    contrib/ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/providers/s3/proto
    contrib/ydb/library/yql/providers/common/http_gateway
    yql/essentials/public/issue
    contrib/ydb/library/yql/udfs/common/clickhouse/client
)

IF (CLANG AND NOT WITH_VALGRIND)

    CFLAGS (
        -DARCADIA_BUILD -DUSE_PARQUET
    )

    SRCS(
        events.cpp
    )

ENDIF()

END()
