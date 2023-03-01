DEPENDS(
    yt/yt/tests/cpp/recipe
    yt/yt/packages/tests_package
)

REQUIREMENTS(
    cpu:4
    ram:10
    ram_disk:10
)

USE_RECIPE(
    yt/yt/tests/cpp/recipe/recipe --cluster-config config.yson
)
