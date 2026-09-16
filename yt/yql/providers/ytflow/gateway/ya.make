LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/threading/future
    library/cpp/yson/node
    library/cpp/yt/misc
    library/cpp/yt/memory
    library/cpp/yt/string

    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/core/file_storage
    yql/essentials/minikql
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/expr
    yql/essentials/utils
    yql/essentials/utils/log

    yt/yql/providers/ytflow/common
    yt/yql/providers/ytflow/expr_nodes
    yt/yql/providers/ytflow/integration/interface
    yt/yql/providers/ytflow/integration/proto
    yt/yql/providers/ytflow/lambda_builder
    yt/yql/providers/ytflow/provider

    yt/yt/client
    yt/yt/client/cache
    yt/yt/core
    yt/yt/library/arcadia_future_interop
    yt/yt/flow/library/cpp/pipeline_helpers
    yt/yt/flow/library/cpp/common

)

SRCS(
    yql_ytflow.cpp
    yql_ytflow_config_clusters.cpp
    yql_ytflow_mkql_compiler.cpp
    yql_ytflow_pipeline_spec.cpp
    yql_ytflow_prepare_common.cpp
    yql_ytflow_prepare_yt.cpp
    yql_ytflow_schema.cpp
    yql_ytflow_yt_clients_cache.cpp
    yql_ytflow_utils.cpp
    yql_ytflow_worker_config.cpp
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ELSE()
    SRCS(
        yql_ytflow_no_logbroker_cm_clients_cache.cpp
        yql_ytflow_no_monium_clients_cache.cpp
        yql_ytflow_no_prepare_logbroker.cpp
        yql_ytflow_no_prepare_monium.cpp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
