RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/scheme
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/formats/arrow/simple_builder
    contrib/ydb/core/formats/arrow/dictionary
    contrib/ydb/core/formats/arrow/transformer
    contrib/ydb/core/formats/arrow/reader
    library/cpp/actors/core
    contrib/ydb/library/arrow_kernels
    contrib/ydb/library/binary_json
    contrib/ydb/library/dynumber
    contrib/ydb/library/services
)

IF (OS_WINDOWS)
    ADDINCL(
        contrib/ydb/library/yql/udfs/common/clickhouse/client/base
        contrib/ydb/library/arrow_clickhouse
    )
ELSE()
    PEERDIR(
        contrib/ydb/library/arrow_clickhouse
    )
    ADDINCL(
        contrib/ydb/library/arrow_clickhouse
    )
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_batch_builder.cpp
    arrow_filter.cpp
    arrow_helpers.cpp
    converter.cpp
    converter.h
    custom_registry.cpp
    input_stream.h
    merging_sorted_input_stream.cpp
    merging_sorted_input_stream.h
    one_batch_input_stream.h
    permutations.cpp
    program.cpp
    replace_key.cpp
    size_calcer.cpp
    sort_cursor.h
    ssa_program_optimizer.cpp
    special_keys.cpp
)

END()
