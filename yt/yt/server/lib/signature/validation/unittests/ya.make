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
    yt/yt/server/lib/signature/common
    yt/yt/server/lib/signature/common/test_helpers
    yt/yt/server/lib/signature/validation
)

SIZE(SMALL)

END()
