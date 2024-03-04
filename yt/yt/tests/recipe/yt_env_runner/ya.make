PY3_PROGRAM(yt_env_runner)

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/yt/tests/recipe/common
    yt/yt/tests/library
    yt/python/yt/test_helpers
    library/python/testing/recipe
)

END()
