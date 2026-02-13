PY3TEST()

PEERDIR(
    yt/admin/ytsaurus_ci
)

TEST_SRCS(
    conftest.py
    test_process_scenario.py
    test_matrix.py
)

SIZE(SMALL)

END()
