GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    process_function_ut.cpp
    transform_ordered_source_adapter_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/library/query/engine
    yt/yt/client/cache
    yt/yt/client/unittests/mock
)

END()
