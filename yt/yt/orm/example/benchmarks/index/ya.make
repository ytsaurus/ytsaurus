G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/benchmarks/index/recipe/recipe.inc)

SRCS(
    index.cpp
)

PEERDIR(
    yt/yt/orm/example/client/native

    yt/yt/orm/client/native
    yt/yt/orm/client/objects

    yt/yt/core

    library/cpp/testing/gtest_extensions
)

END()

RECURSE(recipe)
