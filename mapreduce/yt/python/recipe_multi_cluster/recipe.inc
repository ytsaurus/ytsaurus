DEPENDS(
    mapreduce/yt/python/recipe_multi_cluster
    yt/packages/19_4
)

DEFAULT(YT_REPLICAS primary)

USE_RECIPE(mapreduce/yt/python/recipe_multi_cluster/yt_recipe -R ${YT_REPLICAS})
