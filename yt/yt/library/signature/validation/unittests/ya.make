GTEST(unittester-signature-validation)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    cypress_key_reader_ut.cpp
    signature_validator_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/library/signature/common
    yt/yt/library/signature/common/test_helpers
    yt/yt/library/signature/validation
)

SIZE(SMALL)

END()
