PY3_LIBRARY()

PEERDIR(
    yt/python/yt/environment/arcadia_interop

    yt/python/yt/wrapper

    yt/python/yt

    library/python/testing/recipe
    library/python/testing/yatest_common
)

PY_SRCS(
    NAMESPACE yt.orm.recipe

    recipe.py
)

END()
