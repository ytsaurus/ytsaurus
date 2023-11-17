LIBRARY()

PEERDIR(
    library/cpp/string_utils/parse_size
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/dq/actors
)

GENERATE_ENUM_SERIALIZATION(yql_dq_settings.h)

SRCS(
    attrs.cpp
    yql_dq_common.cpp
    yql_dq_settings.cpp
)

YQL_LAST_ABI_VERSION()

END()
