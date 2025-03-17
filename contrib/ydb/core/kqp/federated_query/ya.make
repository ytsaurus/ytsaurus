LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
    kqp_federated_query_helpers.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/grpc
    contrib/ydb/core/fq/libs/db_id_async_resolver_impl
    contrib/ydb/library/db_pool/protos
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/generic/connector/libcpp
    contrib/ydb/library/yql/providers/solomon/gateway
    yql/essentials/core/dq_integration/transform
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/mkql_dq
)

YQL_LAST_ABI_VERSION()

END()
