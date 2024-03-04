DEPENDS(
    yt/yt/tests/recipe/yt_env_runner
    yt/yt/tests/recipe
    yt/yt/packages/tests_package
)

REQUIREMENTS(
    cpu:4
    ram_disk:4
)

USE_RECIPE(
    yt/yt/tests/recipe/recipe --yt-env-config ${MODDIR}/config.yson
)
