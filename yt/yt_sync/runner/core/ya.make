PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    description.py
    details.py
    log.py
    runner.py
)

PEERDIR(
    yt/yt_sync/sync

    contrib/python/coloredlogs
)

END()

RECURSE_FOR_TESTS(ut)
