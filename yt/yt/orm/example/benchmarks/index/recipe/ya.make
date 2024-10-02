PY3_PROGRAM(benchmark-index-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    yt/yt/orm/example/python/client

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
