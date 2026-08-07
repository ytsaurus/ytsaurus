PY3TEST()

TEST_SRCS(
    test.py
    yt_sync.py
)

SET(YT_CLUSTER_NAMES primary,remote_0)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/pipeline
)

DATA(arcadia/${MODDIR}/pipeline/pipeline_swift.yson)
DATA(arcadia/${MODDIR}/pipeline/pipeline_transform.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

FORK_SUBTESTS()

# Under asan a single test takes 40-70s and the suite has 20+ launches: one shared
# chunk overruns the 600s MEDIUM budget. A few tests per chunk keep headroom.
SPLIT_FACTOR(8)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
