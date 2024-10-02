PY3_PROGRAM(example-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    yt/yt/orm/example/python/local

    yt/yt/orm/python/orm/recipe

    library/python/testing/recipe
)

END()
