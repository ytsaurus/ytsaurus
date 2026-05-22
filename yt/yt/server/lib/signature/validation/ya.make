LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    cypress_key_reader.cpp
    signature_validator.cpp
)

PEERDIR(
    yt/yt/server/lib/signature/common
    yt/yt/client
    yt/yt/core
)

CHECK_DEPENDENT_DIRS(
    DENY
    PEERDIRS
    yt/yt/ytlib
)

END()

RECURSE_FOR_TESTS(
    unittests
)
