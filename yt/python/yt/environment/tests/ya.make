PY3TEST()

TEST_SRCS(
    test_tls_helpers.py
)

PEERDIR(
    yt/python/yt/environment
    yt/python/yt/environment/arcadia_interop
)

DEPENDS(
    contrib/libs/openssl/apps
)

SIZE(MEDIUM)

END()
