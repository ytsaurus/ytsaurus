LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    state_joiner_functions.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/common
)

END()
