PY3_PROGRAM(yt_sync)

STYLE_PYTHON()

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/yt_sync/runner/core
)

END()

RECURSE_FOR_TESTS(ut)
