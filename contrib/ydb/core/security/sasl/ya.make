LIBRARY()

PEERDIR(
    contrib/libs/openssl
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    contrib/ydb/library/actors/core
    contrib/ydb/library/login/hashes_checker
    contrib/ydb/library/login/password_checker
    contrib/ydb/library/login/sasl
)

SRCS(
    hasher.cpp
)

END()
