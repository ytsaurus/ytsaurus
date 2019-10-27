DEPENDS(
    mapreduce/yt/python/recipe
    yt/packages/19_4
    contrib/tools/python
)

REQUIREMENTS(
    ram_disk:4
)

USE_RECIPE(
    mapreduce/yt/python/recipe/yt_recipe
)
