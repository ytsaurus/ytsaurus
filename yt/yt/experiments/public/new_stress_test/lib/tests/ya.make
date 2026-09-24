PY3TEST()

PEERDIR(
    yt/yt/experiments/public/new_stress_test/lib
)

TEST_SRCS(
    test_queue_cumulative_data_weight.py
    test_queue_hunk_storage.py
    test_queue_replicas.py
)

END()
