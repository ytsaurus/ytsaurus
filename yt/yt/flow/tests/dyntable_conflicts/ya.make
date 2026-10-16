PY3TEST()

TEST_SRCS(
    test_conflicts.py
)

SET(YT_DB_MODE chaos)
SET(YT_TABLET_CELL_BUNDLE_NAME flow-bundle)

INCLUDE(${ARCADIA_ROOT}/yt/recipe/chaos/recipe.inc)

PEERDIR(
    yt/python/client_with_rpc
)

REQUIREMENTS(
    cpu:4
    ram:16
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
