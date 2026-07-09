LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    dynamic_table_multiplexer_computation.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/misc
    yt/yt/client
    yt/yt/core
)

END()

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    RECURSE(
        tests
    )
ENDIF()
