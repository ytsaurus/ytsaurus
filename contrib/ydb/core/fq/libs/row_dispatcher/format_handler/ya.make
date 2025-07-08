LIBRARY()

SRCS(
    format_handler.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/row_dispatcher/events
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/common
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/filters
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/parsers

    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util

    contrib/ydb/library/yql/dq/common
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
