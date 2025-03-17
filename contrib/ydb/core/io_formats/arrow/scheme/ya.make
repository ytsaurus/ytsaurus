LIBRARY()

SRCS(
    scheme.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/library/formats/arrow/csv/converter
    contrib/ydb/core/scheme_types
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
