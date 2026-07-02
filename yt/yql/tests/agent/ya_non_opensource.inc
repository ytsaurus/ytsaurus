PEERDIR(
    kikimr/public/sdk/python/ydb_v3_new_behavior

    library/python/ydb/federated_topic_client

    logbroker/tools/lib/recipe_helpers

    yt/yql/tests/common/test_framework

    yt/yt/flow/yandex/extensions/monium/python
)

TEST_SRCS(
    test_ytflow.py
    test_qtworker.py
)

DEPENDS(
    kikimr/public/tools/federation_recipe
    kikimr/public/tools/package/stable

    logbroker/config-manager/bin

    yt/yql/tests/agent/throwing_udf
    yt/yql/tools/ytflow_worker

    yt/yql/tools/qtworker
    yql/essentials/tools/udf_resolver
    yql/essentials/tools/udf_dep_stub
)

DATA(
    arcadia/yt/yql/cfg/tests/worker.conf
    arcadia/yt/yql/cfg/tests/gateways.conf
    arcadia/yt/yql/cfg/tests/fs.conf
)
