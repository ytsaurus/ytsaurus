GTEST(unittester-ytlib-table-client)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    any_column_ut.cpp
    boolean_column_ut.cpp
    chunk_index_read_controller_ut.cpp
    chunk_meta_extensions_ut.cpp
    column_format_ut.cpp
    columnar_statistics_ut.cpp
    complex_column_ut.cpp
    floating_point_column_ut.cpp
    granule_min_max_filter_ut.cpp
    integer_column_ut.cpp
    meta_aggregating_writer_ut.cpp
    null_column_ut.cpp
    schemaless_blocks_ut.cpp
    schemaless_chunks_ut.cpp
    schemaless_column_ut.cpp
    string_column_ut.cpp
    table_schema_ut.cpp
    timestamp_column_ut.cpp
    value_consumer_ut.cpp
    versioned_blocks_ut.cpp
    versioned_chunks_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/client/unittests/mock
    yt/yt/client/table_client/unittests/helpers
    yt/yt/core/test_framework
    yt/yt/library/query/engine
    yt/yt/ytlib
)

FORK_SUBTESTS(MODULO)

SPLIT_FACTOR(5)

SIZE(MEDIUM)

END()
