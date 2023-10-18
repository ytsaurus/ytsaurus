PY3_LIBRARY()

# For opensource.
PY_SRCS(
    __init__.py
)

TEST_SRCS(
    proxy_format_config.py
    test_grpc_proxy.py
    test_http_proxy.py
    test_cypress_cookie_auth.py
    test_cypress_token_auth.py
    test_proxy_roles.py
    test_rpc_proxy.py
    test_oauth.py
)

PEERDIR(
    yt/yt/tests/conftest_lib
    yt/yt/tests/integration/helpers
)

IF (NOT OPENSOURCE)
    PEERDIR(
        library/python/tvmauth
    )

    TEST_SRCS(
        test_yandex_auth.py
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    bin
)
