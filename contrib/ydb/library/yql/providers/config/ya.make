LIBRARY()

SRCS(
    yql_config_provider.cpp
    yql_config_provider.h
)

PEERDIR(
    library/cpp/json
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/fetch
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/activation
)

YQL_LAST_ABI_VERSION()

END()
