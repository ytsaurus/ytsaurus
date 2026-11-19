LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    authenticator.cpp
    computation_controller.cpp
    resource.cpp
    source_context.cpp
    state.cpp
)

PEERDIR(
    library/cpp/testing/gtest_extensions
    yt/yt/flow/library/cpp/common
)

END()
