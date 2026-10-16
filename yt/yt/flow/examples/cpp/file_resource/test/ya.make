PY3TEST()

TEST_SRCS(
    test_file_resource.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

DEPENDS(
    ${MODDIR}/..
)

DATA(
    arcadia/${MODDIR}/../pipeline-yt-file.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

SIZE(MEDIUM)

END()
