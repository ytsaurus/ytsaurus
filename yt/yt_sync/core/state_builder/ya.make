PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    actual.py
    base.py
    desired.py
    desired_deprecated.py
    fake_actual.py
    helpers.py
)

PEERDIR(
    yt/yt_sync/core/client
    yt/yt_sync/core/helpers
    yt/yt_sync/core/model
    yt/yt_sync/core/settings
    yt/yt_sync/core/spec
    yt/yt_sync/core/table_filter

    yt/python/client

    library/python/confmerge
)

END()

RECURSE_FOR_TESTS(
    ut
)
