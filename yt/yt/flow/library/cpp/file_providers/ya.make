LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    file_provider_base.cpp
    GLOBAL local_file_provider.cpp
    GLOBAL yt_directory_last_file_provider.cpp
    GLOBAL yt_file_provider.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/client/cache
    yt/yt/core
    yt/yt/flow/library/cpp/file_storage
    library/cpp/yt/memory
)

END()

RECURSE_FOR_TESTS(
    unittests
)
