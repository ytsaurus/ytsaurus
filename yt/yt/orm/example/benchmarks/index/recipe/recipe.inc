USE_RECIPE(
    yt/yt/orm/example/python/recipe/bin/example-recipe
)

USE_RECIPE(yt/yt/orm/example/benchmarks/index/recipe/benchmark-index-recipe)

DEPENDS(
    ${ARCADIA_ROOT}/yt/yt/orm/example/benchmarks/index/recipe
    ${ARCADIA_ROOT}/yt/yt/orm/example/python/recipe/bin
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_make_integration.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/python/ya_programs.make.inc)
