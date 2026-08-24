PY3TEST()

SIZE(SMALL)

PEERDIR(
    yt/python/client_with_admin
)

TEST_SRCS(
    test_bundle_controller.py
    test_metrics.py
)

END()
