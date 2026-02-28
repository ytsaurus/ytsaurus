PEERDIR(
    kikimr/public/sdk/python/ydb_v3_new_behavior

    library/python/ydb/federated_topic_client

    logbroker/tools/lib/recipe_helpers

    yt/yql/tests/common/test_framework
)

TEST_SRCS(
    test_ytflow.py
)

DEPENDS(
    kikimr/public/tools/federation_recipe
    kikimr/public/tools/package/stable
    logbroker/config-manager/bin

    yt/yql/tests/agent/throwing_udf
    yt/yql/tools/ytflow_worker
)
