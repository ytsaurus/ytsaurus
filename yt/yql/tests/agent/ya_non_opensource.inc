PEERDIR(
    contrib/python/dirty-equals

    kikimr/public/sdk/python/ydb_v3_new_behavior

    library/python/ydb/federated_topic_client

    logbroker/public/api/admin
    logbroker/tools/lib/recipe_helpers

    yql/tools/yqlworker/interface/proto

    yt/yt/flow/yandex/extensions/monium/python
)

TEST_SRCS(
    test_qtworker.py
    test_ytflow_non_opensource.py
)

DEPENDS(
    kikimr/public/tools/federation_recipe
    kikimr/public/tools/package/stable

    logbroker/config-manager/bin

    yt/yql/tools/qtworker
    yql/essentials/tools/udf_resolver
    yql/essentials/tools/udf_dep_stub
)

DATA(
    arcadia/yt/yql/cfg/tests/worker.conf
    arcadia/yt/yql/cfg/tests/gateways.conf
    arcadia/yt/yql/cfg/tests/fs.conf
)
