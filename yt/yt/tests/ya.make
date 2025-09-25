RECURSE(
    integration
    compression
    cuda_core_dump_simulator
    ysonperf
    local_s3_recipe
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
