LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL builtin_function_profiler.cpp
    builtin_registry.cpp
    GLOBAL column_evaluator.cpp
    GLOBAL column_evaluator_cache.cpp
    evaluation_helpers.cpp
    program.cpp
    GLOBAL query_evaluator.cpp
    registry.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/query/base
    yt/yt/library/query/engine_api
)

PROVIDES(YT_QUERY_ENGINE)

# Portable query engine is private to YQL over YTFlow.
PEERDIR_FROM_POLICY(
    ALLOW
    yt/yt/flow/library/cpp/runner/portable_engine$
    yt/yt/library/query/portable_engine/.*
    yt/yql/providers/ytflow/.*
    yt/yql/tools/ytflow_worker
)

PEERDIR_FROM_POLICY(DENY .*)

END()

RECURSE_FOR_TESTS(
    unittests
)
