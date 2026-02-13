LIBRARY()

ADDINCL(
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    contrib/ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

SRCS(
    yql_arrow_push_down.cpp
    yql_s3_actors_factory_impl.cpp
    yql_s3_actors_util.cpp
    yql_s3_applicator_actor.cpp
    yql_s3_raw_read_actor.cpp
    yql_s3_write_actor.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    contrib/libs/poco/Util
    contrib/ydb/library/actors/http
    library/cpp/protobuf/util
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/xml/document
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/util
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/common/arrow
    contrib/ydb/library/yql/providers/common/arrow/interface
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/generic/pushdown
    contrib/ydb/library/yql/providers/s3/actors_factory
    contrib/ydb/library/yql/providers/s3/common
    contrib/ydb/library/yql/providers/s3/compressors
    contrib/ydb/library/yql/providers/s3/credentials
    contrib/ydb/library/yql/providers/s3/events
    contrib/ydb/library/yql/providers/s3/object_listers
    contrib/ydb/library/yql/providers/s3/proto
    contrib/ydb/library/yql/providers/s3/range_helpers
    contrib/ydb/library/yql/udfs/common/clickhouse/client
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/providers/common/schema/mkql
    yql/essentials/public/issue
    yql/essentials/public/types
    yql/essentials/utils
)

IF (CLANG AND NOT WITH_VALGRIND)

    SRCS(
        yql_arrow_column_converters.cpp
        yql_s3_decompressor_actor.cpp
        yql_s3_read_actor.cpp
        yql_s3_source_queue.cpp
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

RECURSE_FOR_TESTS(
    ut
)
