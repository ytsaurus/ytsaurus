PY3TEST()

TEST_SRCS(
    test_final_diagnostics.py
    test_internal_states.py
    test_local_clusters.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
)

END()
