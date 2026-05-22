GTEST(unittester-signature-generation)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    key_rotator_ut.cpp
    signature_generator_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/server/lib/signature/common
    yt/yt/server/lib/signature/common/test_helpers
    yt/yt/server/lib/signature/generation
)

SIZE(SMALL)

END()
