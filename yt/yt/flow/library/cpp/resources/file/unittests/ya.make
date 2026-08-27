GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    file_resource_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client/cache
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/file_sources
    yt/yt/flow/library/cpp/file_storage
    yt/yt/flow/library/cpp/resources/file
)

END()
