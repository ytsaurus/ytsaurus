PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    conftest.py
    test_portal.py
    test_common.py
)

PEERDIR(
    yt/python/client
    yt/python/yt/test_helpers
    yt/python/yt/local
    yt/python/yt/environment/arcadia_interop
    yt/yt/python/yt_driver_bindings
)

DEPENDS(
    yt/cron/clear_tmp
    yt/yt/packages/tests_package
)

END()

