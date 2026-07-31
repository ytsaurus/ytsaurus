GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    file_resource_ut.cpp
    local_file_source_ut.cpp
    yt_directory_last_file_source_ut.cpp
    yt_file_source_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client/cache
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/resources/file
)

END()
