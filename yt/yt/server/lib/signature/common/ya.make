LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    crypto.cpp
    key_info.cpp
    key_pair.cpp
    signature_header.cpp
    signature_preprocess.cpp
)

PEERDIR(
    yt/yt/core
    contrib/libs/libsodium
    library/cpp/string_utils/secret_string
)

END()

RECURSE_FOR_TESTS(
    unittests
)
