GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    local_file_provider_ut.cpp
    yt_directory_last_file_provider_ut.cpp
    yt_file_provider_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client/cache
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/file_providers
)

END()
