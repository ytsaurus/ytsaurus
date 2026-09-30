PY3TEST()

# ZooKeeper must be included before ClickHouse: it sets RECIPE_ZOOKEEPER_HOST,
# which makes the ClickHouse recipe use config_with_zookeeper.xml (with the
# {shard}/{replica} macros) so ReplicatedMergeTree block dedup works.
INCLUDE(${ARCADIA_ROOT}/library/recipes/zookeeper/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/library/recipes/clickhouse/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    clickhouse_cluster.py
    test_clickhouse.py
    yt_sync.py
)

PEERDIR(
    contrib/python/clickhouse-driver
    yt/yt/flow/library/python/integration_test_base
)

DEPENDS(
    yt/yt/flow/extensions/clickhouse/test/pipeline
)

DATA(arcadia/yt/yt/flow/extensions/clickhouse/test/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
