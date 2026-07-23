PY3_LIBRARY()

PY_SRCS(
    common.py
    compare.py
    dq_file.py
    hybrid_file.py
    yt_file.py
    yt_interaction.py
    yt_setup.py
    ytflow_interaction.py
)

PEERDIR(
    contrib/python/urllib3
    library/python/port_manager
    yql/essentials/core/file_storage/proto
    yql/essentials/providers/common/proto
    yql/essentials/tests/common/test_framework
    yt/yql/tests/common/test_framework
)

END()
