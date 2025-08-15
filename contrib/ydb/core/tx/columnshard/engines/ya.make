RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    metadata_accessor.cpp
    column_engine_logs.cpp
    column_engine.cpp
    db_wrapper.cpp
    filter.cpp
    defs.cpp
)

GENERATE_ENUM_SERIALIZATION(column_engine_logs.h)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/base
    contrib/ydb/core/formats
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/engines/changes
    contrib/ydb/core/tx/columnshard/engines/loading
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/engines/predicate
    contrib/ydb/core/tx/columnshard/engines/protos
    contrib/ydb/core/tx/columnshard/engines/reader
    contrib/ydb/core/tx/columnshard/engines/storage
    contrib/ydb/core/tx/columnshard/tracing
    contrib/ydb/core/tx/program

    # for NYql::NUdf alloc stuff used in binary_json
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
