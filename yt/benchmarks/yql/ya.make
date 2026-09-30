RECURSE(
    run
)

IF (NOT OPENSOURCE)
    RECURSE(
        analyze
        compare
        job_profiler
    )
ENDIF()
