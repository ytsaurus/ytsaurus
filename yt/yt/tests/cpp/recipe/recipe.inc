DEPENDS(
    ${YT_ROOT}/yt/tests/cpp/recipe
    ${YT_ROOT}/yt/packages/tests_package
)

REQUIREMENTS(
    cpu:4
    ram_disk:4
)

USE_RECIPE(
    ${YT_ROOT}/yt/tests/cpp/recipe/recipe --cluster-config config.yson
)
