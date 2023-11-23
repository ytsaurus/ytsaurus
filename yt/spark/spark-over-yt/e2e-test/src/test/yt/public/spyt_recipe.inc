DEPENDS(
    mapreduce/yt/python/recipe
    yt/packages/latest

    yt/spark/spark-over-yt/e2e-test/src/test/yt/data
)

PEERDIR(
    yt/spark/spark-over-yt/e2e-test/src/test/yt/public
)

USE_RECIPE(
    mapreduce/yt/python/recipe/yt_recipe
    --master-config "{cypress_manager={default_table_replication_factor=1;default_file_replication_factor=1}}"
    --node-config   "{exec_node={job_controller={resource_limits={cpu=2;memory=4294967296}}}}"
    --master-count 1
    --node-count 3
)
