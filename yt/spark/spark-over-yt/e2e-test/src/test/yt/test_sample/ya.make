PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/e2e-test/src/test/yt/public/spyt_recipe.inc)

PEERDIR(
    contrib/python/pytest-timeout
)

TEST_SRCS(
    test_jobs.py
)

DATA(
    arcadia/yt/spark/spark-over-yt/e2e-test/src/test/yt/jobs/id.py
)

SIZE(MEDIUM)

END()
