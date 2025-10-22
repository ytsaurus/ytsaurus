LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    misra_gries.cpp
    config.cpp
)

PEERDIR(
    library/cpp/yt/memory
    yt/yt/core
)

END()

IF (NOT OPENSOURCE AND OS_LINUX)
    RECURSE(
        benchmark
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
)
