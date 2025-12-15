INCLUDE(${ARCADIA_ROOT}/kikimr/public/tools/federation_recipe/recipe.inc)

PEERDIR(
    library/python/ydb/federated_topic_client
    kikimr/public/sdk/python/ydb_v3_new_behavior
)

TEST_SRCS(
    test_ytflow.py
)

DEPENDS(
    yt/yql/tests/agent/throwing_udf
    yt/yql/tools/ytflow_worker
)
