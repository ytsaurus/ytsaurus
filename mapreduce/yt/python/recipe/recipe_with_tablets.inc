DEPENDS(
    mapreduce/yt/python/recipe
    yt/packages/19_4
    contrib/tools/python
)

USE_RECIPE(
    mapreduce/yt/python/recipe/yt_recipe --wait-tablet-cell-initialization
)

