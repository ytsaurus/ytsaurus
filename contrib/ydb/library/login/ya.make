LIBRARY()

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/libs/protobuf
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    contrib/ydb/library/login/protos
    contrib/ydb/library/login/password_checker
    contrib/ydb/library/login/account_lockout
    contrib/ydb/library/login/cache
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
    password_checker
    account_lockout
    cache
)
