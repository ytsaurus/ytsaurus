RECURSE_FOR_TESTS(ut)

LIBRARY()

SRCS(
    csv_ydb_dump.cpp
    csv_arrow.cpp
)

CFLAGS(
    -Wno-unused-parameter
)

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/string_utils/quote
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/scheme
    contrib/ydb/library/binary_json
    contrib/ydb/library/dynumber
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/public/lib/scheme_types
)

YQL_LAST_ABI_VERSION()

END()
