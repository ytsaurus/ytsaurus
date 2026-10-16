LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    cypress_key_writer.cpp
    key_rotator.cpp
    signature_generator.cpp
)

PEERDIR(
    yt/yt/library/signature/common
    yt/yt/core
    yt/yt/client
)

CHECK_DEPENDENT_DIRS(
    DENY
    PEERDIRS
    yt/yt/ytlib
    yt/yt/server
)

END()

RECURSE_FOR_TESTS(
    unittests
)
