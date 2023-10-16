GTEST(unittester-library-auth_server)

ALLOCATOR(YT)

SRCS(
    blackbox_ut.cpp
    mock_http_server.cpp
    secret_vault_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/build

    yt/yt/core/test_framework

    yt/yt/library/auth_server
    yt/yt/library/tvm/service/mock

    library/cpp/http/server
)

# Somehow tests in this suite work tens of seconds.

SIZE(MEDIUM)

FORK_TESTS()

SPLIT_FACTOR(5)

END()
