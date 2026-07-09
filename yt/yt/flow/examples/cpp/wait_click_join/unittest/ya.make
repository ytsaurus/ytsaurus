GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    wait_click_join_functions_ut.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/wait_click_join/lib
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
)

END()
