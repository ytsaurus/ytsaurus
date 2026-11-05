PY3_PROGRAM(yt_recipe)

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/recipe/basic/cluster_factory

    yt/recipe/chaos/lib

    yt/yt/python/yt_driver_bindings

    library/python/testing/recipe
)

END()
