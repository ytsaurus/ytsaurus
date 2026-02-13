LIBRARY()

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/libs/protobuf
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    contrib/ydb/library/login/account_lockout
    contrib/ydb/library/login/cache
    contrib/ydb/library/login/hashes_checker
    contrib/ydb/library/login/protos
    contrib/ydb/library/login/password_checker
    contrib/ydb/library/login/sasl
)

SRCS(
    login.cpp
    login.h
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    account_lockout
    cache
    hashes_checker
    password_checker
    sasl
)
