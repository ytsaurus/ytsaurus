RECURSE(
    integration
    # validate
    compression
    cuda_core_dump_simulator
    ysonperf
    cpp
)

IF (NOT OPENSOURCE)
    RECURSE(recipe)
ENDIF()

RECURSE_FOR_TESTS(
    library
)
