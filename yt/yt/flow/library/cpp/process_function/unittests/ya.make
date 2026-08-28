GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    adapter_test_context.cpp
    process_function_ut.cpp
    registry_ut.cpp
    source_adapter_ut.cpp
    transform_ordered_source_adapter_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/process_function/testing
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/library/profiling/solomon
    yt/yt/library/query/engine
    yt/yt/client/cache
    yt/yt/client/unittests/mock
)

END()
