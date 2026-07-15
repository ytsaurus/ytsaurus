LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    noop_functions.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/computation
)

END()
