PY3_PROGRAM(yt_sync)

STYLE_PYTHON()

PY_SRCS(
    __main__.py
    queues.py
    stages.py
    tables.py
)

PEERDIR(
    yt/yt_sync/runner
)

END()

RECURSE_FOR_TESTS(ut)
