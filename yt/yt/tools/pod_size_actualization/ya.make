RECURSE(
    defragmentation
    lib
    optimization
    optimization/tests
    optimization/yql
)

IF (NOT OPENSOURCE)
    RECURSE(
        bin
        graphs
        cluster_data
    )
    RECURSE_FOR_TESTS(
        graphs/tests
    )
ENDIF()
