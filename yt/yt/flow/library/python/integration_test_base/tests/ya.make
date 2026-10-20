PY3TEST()

TEST_SRCS(
    test_final_diagnostics.py
    test_internal_states.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
)

END()
