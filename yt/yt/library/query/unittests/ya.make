LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL ast_ut.cpp
    GLOBAL ql_computed_columns_ut.cpp
    GLOBAL ql_distributed_ut.cpp
    GLOBAL ql_expressions_ut.cpp
    GLOBAL ql_helpers.cpp
    GLOBAL ql_misc_ut.cpp
    GLOBAL ql_query_ut.cpp
    GLOBAL ql_range_inference_ut.cpp
    GLOBAL ql_range_coordination_ut.cpp
)

IF (OPENSOURCE)
    SRCS(
        disable_web_assembly.cpp
    )
ELSE()
    SRCS(
        disable_web_assembly.cpp
        #enable_web_assembly.cpp
    )
ENDIF()

ADDINCL(
    contrib/libs/sparsehash/src
)

# This flag is required for linking code of bc functions.
# Note that -rdynamic enabled by default in arcadia build, but we do not want to rely on it.
LDFLAGS(-rdynamic)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/library/query/distributed
    yt/yt/library/query/engine
    yt/yt/library/query/engine_api
    yt/yt/library/query/unittests/helpers
    yt/yt/library/query/unittests/udf
    contrib/libs/sparsehash
)

END()

RECURSE(
    v1
    v2
)
