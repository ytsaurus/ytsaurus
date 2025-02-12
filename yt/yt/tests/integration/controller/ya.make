PY3_LIBRARY()

TEST_SRCS(
    test_auto_merge.py
    test_columnar_statistics.py
    test_controller_agent.py
    test_controller_transactions.py
    test_erase_operation.py
    test_fast_intermediate_medium.py
    test_features.py
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
    test_remote_operation.py
    test_shuffle_service.py
    test_sort_operation.py
    test_vanilla_operation.py
)

IF (NOT OPENSOURCE)
    PEERDIR(
        yt/yt/tests/integration/helpers
    )

    TEST_SRCS(
        test_controller_agent_heap_profile.py
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    bin
)
