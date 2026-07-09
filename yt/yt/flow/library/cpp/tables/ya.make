LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    common.cpp
    compact_output_messages.cpp
    compact_partition_output_messages.cpp
    context.cpp
    input_messages.cpp
    key_states.cpp
    key_visitor_states.cpp
    partition_states.cpp
    state.cpp
    timers.cpp
    transaction_manager.cpp
)

PEERDIR(
    library/cpp/yt/misc
    yt/yt/flow/lib/serializer
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/library/profiling/sensors_owner
)

END()

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
