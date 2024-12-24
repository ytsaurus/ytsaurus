RECURSE(
    integration
    # validate
    compression
    cuda_core_dump_simulator
    ysonperf
)

IF (NOT SANITIZER_TYPE)
    RECURSE(
        cpp
    )
ENDIF()

IF (NOT OPENSOURCE)
    RECURSE(recipe)
ENDIF()

RECURSE_FOR_TESTS(
    library
)
