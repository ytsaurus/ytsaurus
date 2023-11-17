RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    column_engine_logs.cpp
    column_engine.cpp
    column_features.cpp
    db_wrapper.cpp
    index_info.cpp
    filter.cpp
    portion_info.cpp
    tier_info.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/base
    contrib/ydb/core/formats
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/engines/reader
    contrib/ydb/core/tx/columnshard/engines/predicate
    contrib/ydb/core/tx/columnshard/engines/storage
    contrib/ydb/core/tx/columnshard/engines/insert_table
    contrib/ydb/core/tx/columnshard/engines/changes
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/formats/arrow/compression
    contrib/ydb/core/tx/program

    # for NYql::NUdf alloc stuff used in binary_json
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
