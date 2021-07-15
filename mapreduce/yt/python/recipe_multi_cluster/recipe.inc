DEPENDS(
    mapreduce/yt/python/recipe_multi_cluster
    yt/packages/19_4
)

DEFAULT(YT_REPLICAS primary)
DEFAULT(YT_REPLICA_CONFIG {})

USE_RECIPE(mapreduce/yt/python/recipe_multi_cluster/yt_recipe -R ${YT_REPLICAS} --replica-config ${YT_REPLICA_CONFIG})
