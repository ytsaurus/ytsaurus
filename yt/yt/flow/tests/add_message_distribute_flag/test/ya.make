PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

TEST_SRCS(
    test_distribute.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/..
    yt/yt/flow/bin/flow_server
)

DATA(arcadia/${MODDIR}/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
    ram_disk:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
