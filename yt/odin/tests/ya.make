PY3TEST()

TEST_SRCS(
    __init__.py
    conftest.py
    test_alerts.py
    test_common.py
    test_db.py
    test_juggler.py
    test_logserver.py
    test_odin.py
)

PEERDIR(
    yt/odin/lib/yt_odin/logserver
    yt/odin/lib/yt_odin/odinserver
    yt/odin/lib/yt_odin/storage
    yt/odin/lib/yt_odin/test_helpers
    yt/python/client
    yt/python/yt/environment/arcadia_interop
    contrib/python/mock
)

DATA(
    arcadia/yt/odin/tests/data
)

DEPENDS(
    yt/odin/tests/data/checks/available
    yt/odin/tests/data/checks/invalid
    yt/odin/tests/data/checks/long
    yt/odin/tests/data/checks/partial
    yt/odin/tests/data/checks/with_invalid_return_value
    yt/odin/tests/data/checks/with_options
    yt/odin/tests/data/checks/with_secrets

    yt/odin/bin/yt_odin

    yt/yt/packages/tests_package
)

SIZE(MEDIUM)

END()

RECURSE(
    data
)
