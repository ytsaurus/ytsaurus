LIBRARY()

ADDINCL(
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    contrib/ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

SRCS(
    yql_s3_actors_util.cpp
    yql_s3_applicator_actor.cpp
    yql_s3_sink_factory.cpp
    yql_s3_source_factory.cpp
    yql_s3_write_actor.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/libs/poco/Util
    library/cpp/actors/http
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/xml/document
    contrib/ydb/core/fq/libs/events
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/public/types
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/common/arrow
    contrib/ydb/library/yql/providers/common/arrow/interface
    contrib/ydb/library/yql/providers/s3/common
    contrib/ydb/library/yql/providers/s3/compressors
    contrib/ydb/library/yql/providers/s3/object_listers
    contrib/ydb/library/yql/providers/s3/proto
    contrib/ydb/library/yql/udfs/common/clickhouse/client
)

IF (CLANG AND NOT WITH_VALGRIND)

    SRCS(
        yql_s3_read_actor.cpp
    )

    PEERDIR(
        contrib/ydb/library/yql/providers/s3/range_helpers
        contrib/ydb/library/yql/providers/s3/serializations
    )

    CFLAGS (
        -DARCADIA_BUILD -DUSE_PARQUET
    )

ENDIF()

END()
