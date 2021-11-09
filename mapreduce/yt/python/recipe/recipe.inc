DEPENDS(
    mapreduce/yt/python/recipe
    yt/packages/latest
    contrib/tools/python
)

REQUIREMENTS(
    ram_disk:4
)

USE_RECIPE(
    mapreduce/yt/python/recipe/yt_recipe
)
