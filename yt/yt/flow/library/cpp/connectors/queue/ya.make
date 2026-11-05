LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    queue_info.cpp
    sink.cpp
    source.cpp
    spec.cpp
    tablet_index_evaluator.cpp
    tablet_router.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/resources
    yt/yt/core
    yt/yt/client
    yt/yt/library/query/engine_api
)

END()

RECURSE_FOR_TESTS(
    unittests
    tests
)

IF (NOT OPENSOURCE)
    # Uses replicated tables — out of yt_sync_mini's scope.
    RECURSE_FOR_TESTS(
        tests_replicated
    )
ENDIF()
