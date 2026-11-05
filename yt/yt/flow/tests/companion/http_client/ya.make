PY3TEST()

STYLE_PYTHON()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_http_client.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
)

DEPENDS(
    ${MODDIR}/companion
    yt/yt/flow/bin/flow_server
)

DATA(
    arcadia/${MODDIR}/pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
    ram_disk:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    companion
    python
)
