LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    file_resource.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/file_sources
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
