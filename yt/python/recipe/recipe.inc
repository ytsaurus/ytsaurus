DEPENDS(
    yt/python/recipe
    yt/packages/19_4
)

DEFAULT(YT_REPLICAS primary)
DEFAULT(YT_ACL_GROUPS )
USE_RECIPE(yt/python/recipe/yt_recipe -R ${YT_REPLICAS} -G ${YT_ACL_GROUPS})
