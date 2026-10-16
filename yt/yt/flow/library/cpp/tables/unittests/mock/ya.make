LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    compact_output_messages.cpp
    compact_partition_output_messages.cpp
    input_messages.cpp
    key_states.cpp
    key_visitor_states.cpp
    partition_states.cpp
    timers.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/tables
)

END()
