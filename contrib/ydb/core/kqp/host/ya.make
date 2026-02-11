LIBRARY()

SRCS(
    kqp_explain_prepared.cpp
    kqp_gateway_proxy.cpp
    kqp_host.cpp
    kqp_runner.cpp
    kqp_transform.cpp
    kqp_translate.cpp
    kqp_statement_rewrite.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/gateway/utils
    contrib/ydb/core/kqp/opt
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/dq/helper
    contrib/ydb/library/yql/providers/generic/provider
    contrib/ydb/library/yql/providers/pq/provider
    contrib/ydb/library/yql/providers/s3/expr_nodes
    yql/essentials/core
    yql/essentials/core/services
    yql/essentials/minikql/invoke_builtins
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/udf_resolve
    yql/essentials/providers/config
    yql/essentials/providers/pg/provider
    yql/essentials/providers/result/provider
    yql/essentials/sql
    yql/essentials/sql/v0
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
