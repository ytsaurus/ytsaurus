PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    companion_resource_test_base.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
)

END()
