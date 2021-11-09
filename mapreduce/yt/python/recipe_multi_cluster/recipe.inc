DEPENDS(
    mapreduce/yt/python/recipe_multi_cluster
    yt/packages/latest
)

DEFAULT(YT_REPLICAS primary)
DEFAULT(YT_REPLICAS_CONFIG {})

USE_RECIPE(mapreduce/yt/python/recipe_multi_cluster/yt_recipe -R ${YT_REPLICAS} --replicas-config ${YT_REPLICAS_CONFIG})
