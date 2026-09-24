LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    dynamic_table_multiplexer_process_function.cpp
    multiplexer_process_function.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/process_function
    yt/yt/client
    yt/yt/client/cache
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        benchmarks
    )
ENDIF()

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    RECURSE(
        tests
    )
ENDIF()
