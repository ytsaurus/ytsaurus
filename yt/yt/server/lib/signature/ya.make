LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    crypto.cpp
    cypress_key_store.cpp
    components.cpp
    key_info.cpp
    key_pair.cpp
    key_rotator.cpp
    signature_generator.cpp
    signature_header.cpp
    signature_preprocess.cpp
    signature_validator.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
    contrib/libs/libsodium
    library/cpp/string_utils/secret_string
)

END()

RECURSE_FOR_TESTS(
    unittests
)
