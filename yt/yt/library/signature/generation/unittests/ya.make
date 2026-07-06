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
    yt/yt/library/signature/common
    yt/yt/library/signature/common/test_helpers
    yt/yt/library/signature/generation
)

SIZE(SMALL)

END()
