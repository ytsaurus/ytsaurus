LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    ql_helpers.cpp
    test_evaluate.cpp
)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/library/query/distributed
    yt/yt/library/query/engine
    yt/yt/library/query/engine_api
    yt/yt/library/query/unittests/helpers
    yt/yt/library/query/unittests/udf
)

IF (OPENSOURCE)
    SRCS(
        disable_web_assembly.cpp
    )
ELSE()
    SRCS(
        enable_web_assembly.cpp
    )
ENDIF()

END()
