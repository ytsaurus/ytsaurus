LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    append_function_implementation.cpp
    builtin_function_profiler.cpp
    cg_cache.cpp
    column_evaluator.cpp
    config.cpp
    coordinator.cpp
    evaluation_helpers.cpp
    evaluator.cpp
    expression_context.cpp
    expression_evaluator.cpp
    new_range_inferrer.cpp
    position_independent_value.cpp
    position_independent_value_transfer.cpp
    GLOBAL query_engine_config.cpp  # TODO(dtorilov): Fix static initializer and remove this GLOBAL.
    query_evaluator.cpp
    range_inferrer.cpp
    shuffling_reader.cpp
    top_collector.cpp
)

ADDINCL(
    contrib/libs/sparsehash/src
)

PEERDIR(
    yt/yt/core
    yt/yt/library/codegen_api
    yt/yt/library/web_assembly/api
    yt/yt/library/query/misc
    yt/yt/library/query/proto
    yt/yt/library/query/base
    yt/yt/client
    library/cpp/yt/memory
    contrib/libs/sparsehash
)

END()
