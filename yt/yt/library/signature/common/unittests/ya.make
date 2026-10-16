GTEST(unittester-signature-common)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    key_info_ut.cpp
    key_pair_ut.cpp
    signature_header_ut.cpp
    signature_preprocess_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/library/signature/common
    yt/yt/library/signature/common/test_helpers
)

SIZE(SMALL)

END()
