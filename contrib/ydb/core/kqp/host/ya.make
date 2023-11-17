LIBRARY()

SRCS(
    kqp_explain_prepared.cpp
    kqp_gateway_proxy.cpp
    kqp_host.cpp
    kqp_runner.cpp
    kqp_transform.cpp
    kqp_translate.cpp
    kqp_type_ann.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/opt
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/config
    contrib/ydb/library/yql/providers/generic/provider
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/providers/s3/provider
)

YQL_LAST_ABI_VERSION()

END()
