LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    file_resource.cpp
    file_source_base.cpp
    GLOBAL local_file_source.cpp
    GLOBAL yt_directory_last_file_source.cpp
    GLOBAL yt_file_source.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/resources
    yt/yt/client/cache
    yt/yt/flow/library/cpp/file_storage
    library/cpp/yt/memory
    library/cpp/yt/threading
)

END()

RECURSE_FOR_TESTS(
    tests
    unittests
)
