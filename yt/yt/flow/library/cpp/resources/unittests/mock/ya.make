LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    yt_client_factory.cpp
)

PEERDIR(
    library/cpp/testing/gtest_extensions
    yt/yt/flow/library/cpp/resources
)

END()
