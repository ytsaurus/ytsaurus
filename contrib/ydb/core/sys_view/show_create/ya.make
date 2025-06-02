LIBRARY()

SRCS(
    create_table_formatter.cpp
    create_view_formatter.cpp
    formatters_common.cpp
    show_create.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/protos
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/tx/columnshard/engines/scheme/defaults/protos
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/sequenceproxy
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/ydb_cli/dump/util
    contrib/ydb/public/sdk/cpp/src/client/types
    yql/essentials/ast
    yql/essentials/public/issue
    yql/essentials/sql/settings
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr3_ansi
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr3
    yql/essentials/sql/v1/proto_parser/antlr3_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
