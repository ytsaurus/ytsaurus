LIBRARY()

SRCS(
    create_table_formatter.cpp
    create_table_formatter.h
    show_create.cpp
    show_create.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/protos
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/ydb_cli/dump/util
    contrib/ydb/public/sdk/cpp/src/client/types
    yql/essentials/ast
)

YQL_LAST_ABI_VERSION()

END()
