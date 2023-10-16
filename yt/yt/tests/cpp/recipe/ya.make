PY3_PROGRAM(recipe)

PY_SRCS(__main__.py)

DEPENDS(yt/yt/packages/tests_package)

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    yt/python/yt/environment
    yt/python/yt/environment/arcadia_interop
    yt/python/yt/local
)

END()
