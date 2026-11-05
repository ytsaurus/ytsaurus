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
    )
ENDIF()
