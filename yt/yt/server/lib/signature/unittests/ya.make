GTEST(unittester-signature-service)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    cypress_key_store_ut.cpp
    helpers.cpp
    key_info_ut.cpp
    key_pair_ut.cpp
    key_rotator_ut.cpp
    signature_header_ut.cpp
    signature_generator_ut.cpp
    signature_validator_ut.cpp
    signature_preprocess_ut.cpp
    stub_keystore.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/server/lib/signature
)

SIZE(SMALL)

END()
