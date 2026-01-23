LIBRARY()

SRCS(
    yql_mock.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/json/yson
    library/cpp/monlib/dynamic_counters
    library/cpp/random_provider
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/actors
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/db_id_async_resolver_impl
    contrib/ydb/core/fq/libs/db_schema
    contrib/ydb/core/fq/libs/shared_resources/interface
    contrib/ydb/core/protos
    contrib/ydb/library/mkql_proto
    yql/essentials/ast
    yql/essentials/core/facade
    yql/essentials/core/services/mounts
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/comp_nodes
    contrib/ydb/library/yql/providers/clickhouse/provider
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/mkql
    yql/essentials/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    contrib/ydb/library/yql/providers/ydb/provider
    yql/essentials/public/issue
    yql/essentials/public/issue/protos
    yql/essentials/sql/settings
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
