PY3_LIBRARY()

PY_SRCS(
    ytflow_interaction.py
)

PEERDIR(
    library/python/port_manager
    yql/essentials/providers/common/proto
    yql/essentials/tests/common/test_framework
    yt/yql/tests/common/test_framework
    yt/yql/tests/sql/runners
)

END()
