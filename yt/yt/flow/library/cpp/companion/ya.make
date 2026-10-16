LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_computation_base.cpp
    GLOBAL register.cpp
    swift_map_companion_computation.cpp
    swift_ordered_source_companion_computation.cpp
    transform_companion_computation.cpp
    transform_ordered_source_companion_computation.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/client
    yt/yt/flow/library/cpp/companion/manager
    yt/yt/flow/library/cpp/companion/resources

    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/misc
)

END()

RECURSE(
    client
    manager
    resources
    server
)

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        benchmark
    )
ENDIF()
