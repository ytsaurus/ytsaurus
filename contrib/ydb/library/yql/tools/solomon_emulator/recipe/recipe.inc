DEPENDS(
    contrib/ydb/library/yql/tools/solomon_emulator/bin
    contrib/ydb/library/yql/tools/solomon_emulator/recipe
)

USE_RECIPE(
    contrib/ydb/library/yql/tools/solomon_emulator/recipe/solomon_recipe --shard my_project/my_cluster/my_service
)
