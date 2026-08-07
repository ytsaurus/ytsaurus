PY3TEST()

TEST_SRCS(
    test_alter.py
    yt_sync.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/pipeline
    yt/python/yt/wrapper/bin/yt_make
)

DATA(arcadia/${MODDIR}/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

FORK_SUBTESTS()

# Under TSAN in the YT team CI each rename scenario runs for minutes; sharing one
# 600s MEDIUM chunk leaves no headroom.
SPLIT_FACTOR(6)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
