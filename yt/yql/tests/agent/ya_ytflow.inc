TEST_SRCS(
    test_ytflow.py
)

DEPENDS(
    yt/yql/tests/agent/throwing_udf
    yt/yql/tools/ytflow_worker
    yql/essentials/udfs/common/datetime2
)

PEERDIR(
    library/python/port_manager
    yt/yql/tests/common/test_framework
)
