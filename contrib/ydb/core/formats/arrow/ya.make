RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/scheme
    contrib/ydb/core/formats/arrow/accessor
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/formats/arrow/dictionary
    contrib/ydb/core/formats/arrow/transformer
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/core/formats/arrow/rows
    contrib/ydb/core/formats/arrow/save_load
    contrib/ydb/core/formats/arrow/splitter
    contrib/ydb/core/formats/arrow/hash
    contrib/ydb/library/actors/core
    contrib/ydb/library/arrow_kernels
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/services
    yql/essentials/core/arrow_kernels/request
)

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_batch_builder.cpp
    arrow_helpers.cpp
    arrow_filter.cpp
    converter.cpp
    converter.h
    permutations.cpp
    size_calcer.cpp
    special_keys.cpp
    process_columns.cpp
)

END()
