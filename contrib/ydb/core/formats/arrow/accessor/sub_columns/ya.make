LIBRARY()

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/core/formats/arrow/accessor/plain
    contrib/ydb/core/formats/arrow/accessor/sparsed
    contrib/ydb/core/formats/arrow/accessor/composite_serial
    contrib/ydb/core/formats/arrow/save_load
    contrib/ydb/core/formats/arrow/common
    contrib/ydb/library/signals
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/formats/arrow/protos
    yql/essentials/types/binary_json
)

SRCS(
    GLOBAL constructor.cpp
    GLOBAL request.cpp
    header.cpp
    partial.cpp
    data_extractor.cpp
    json_extractors.cpp
    accessor.cpp
    direct_builder.cpp
    settings.cpp
    stats.cpp
    others_storage.cpp
    columns_storage.cpp
    iterators.cpp
    signals.cpp
)

YQL_LAST_ABI_VERSION()

CFLAGS(
    -Wno-assume
)

END()

RECURSE_FOR_TESTS(
    ut
)
