PY3TEST()

NO_CHECK_IMPORTS()

TEST_SRCS(
    conftest.py
    test_join_process_function.py
)

PEERDIR(
    yt/yt/flow/library/python/companion/test_harness
    yt/yt/flow/examples/python/wait_click_join
)

SIZE(SMALL)

END()
