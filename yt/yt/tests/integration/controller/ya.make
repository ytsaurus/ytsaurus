PY3_LIBRARY()

TEST_SRCS(
    test_auto_merge.py
    test_columnar_statistics.py
    test_controller_agent.py
    test_erase_operation.py
    test_features.py
    test_intermediate_medium_switch.py
    test_job_experiment.py
    test_job_query.py
    test_job_tracker.py
    test_join_reduce_operation.py
    test_map_operation.py
    test_map_reduce_operation.py
    test_merge_operation.py
    test_partition_tables.py
    test_reduce_operation.py
    test_remote_copy_operation.py
    test_sort_operation.py
    test_vanilla_operation.py
)

PEERDIR(
    yt/yt/tests/integration/helpers
)

END()

RECURSE_FOR_TESTS(
    bin
)
