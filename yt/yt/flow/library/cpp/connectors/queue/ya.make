LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    queue_info.cpp
    sink.cpp
    source.cpp
    spec.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/resources
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    tests
)

IF (NOT OPENSOURCE)
    # Uses replicated tables — out of yt_sync_mini's scope.
    RECURSE_FOR_TESTS(
        tests_replicated
    )
ENDIF()
