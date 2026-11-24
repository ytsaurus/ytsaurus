GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    ../delivery_validation.cpp
    delivery_validation_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/library/query/engine
)

END()
