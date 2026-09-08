LIBRARY()

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_ytflow_computation_pattern.h)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider

    yql/essentials/minikql
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/public/udf
    yql/essentials/sql/pg_dummy
    yql/essentials/utils

    yt/yql/providers/yt/mkql_ytflow
    yt/yql/providers/ytflow/codec
    yt/yql/providers/ytflow/common
    yt/yql/providers/ytflow/comp_nodes
    yt/yql/providers/ytflow/integration/mkql_interface
    yt/yql/providers/ytflow/lambda_builder
    yt/yt/client
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/resources
)

SRCS(
    yql_ytflow_map_computation_graph_with_codecs.cpp
    yql_ytflow_computation_graph_with_codecs_base.cpp
    yql_ytflow_computation_pattern.cpp
    yql_ytflow_computation_pattern_resource.cpp
    yql_ytflow_common_parameters.cpp
    yql_ytflow_function_registry.cpp
    yql_ytflow_function_registry_resource.cpp
    yql_ytflow_update_state_computation_graph_with_codecs.cpp
    yql_ytflow_postprocess_computation_graph_with_codecs.cpp
    yql_ytflow_message_holder.cpp
    yql_ytflow_metrics.cpp
    yql_ytflow_node_factory.cpp
    yql_ytflow_source_transformer.cpp
    yql_ytflow_secure_params.cpp
    yql_ytflow_stream_value.cpp
    yql_ytflow_default_source_transformer.cpp
    yql_ytflow_logbroker_source_transformer.cpp
    yql_ytflow_timing_guard.cpp
    yql_ytflow_utils.cpp
    GLOBAL yql_ytflow_hopping_aggregate.cpp
    GLOBAL yql_ytflow_resources.cpp
    GLOBAL yql_ytflow_source_map.cpp
    GLOBAL yql_ytflow_swift_map.cpp
    GLOBAL yql_ytflow_transform_map.cpp
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
