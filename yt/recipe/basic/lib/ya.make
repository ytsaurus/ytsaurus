PY3_LIBRARY()

PY_SRCS(
    __init__.py
    local_yt.py
    recipe.py
)

PEERDIR(
    contrib/python/deepmerge
    library/python/testing/recipe
    library/python/testing/yatest_common
    yt/python/client
    yt/python/yt/wrapper
)

END()
