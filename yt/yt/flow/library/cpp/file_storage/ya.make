LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    file_storage.cpp
)

PEERDIR(
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/threading
    yt/yt/core
    yt/yt/flow/library/cpp/misc
    yt/yt/library/profiling
)

END()

RECURSE_FOR_TESTS(
    unittests
)
