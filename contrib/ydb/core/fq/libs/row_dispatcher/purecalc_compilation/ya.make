LIBRARY()

SRCS(
    compile_service.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/row_dispatcher/events
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/common
    contrib/ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper

    contrib/ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()
