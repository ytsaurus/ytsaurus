PY3_PROGRAM(recipe)

PY_SRCS(__main__.py)

DEPENDS(
    yt/yt/packages/tests_package
    yt/yt/tests/recipe/yt_env_runner
)

PEERDIR(
    yt/yt/tests/recipe/common
    library/python/testing/recipe
    yt/python/yt/test_helpers
)

END()

RECURSE(
    common
    yt_env_runner
)
