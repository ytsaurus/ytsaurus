PY3TEST()

PEERDIR(
    yt/python/yt/mcp/lib
)

INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)

TEST_SRCS(
    test_variations.py
)

END()

RECURSE_FOR_TESTS(
    yt
)
