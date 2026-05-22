LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    cypress_key_writer.cpp
    key_rotator.cpp
    signature_generator.cpp
)

PEERDIR(
    yt/yt/server/lib/signature/common
    yt/yt/ytlib
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
