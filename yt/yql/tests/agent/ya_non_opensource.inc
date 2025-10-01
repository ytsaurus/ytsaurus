TEST_SRCS(
    test_ytflow.py
)

DEPENDS(
    yt/yql/tests/agent/throwing_udf
    yt/yql/tools/ytflow_worker
)
