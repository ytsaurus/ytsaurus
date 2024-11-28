RECURSE(
    run
)

IF (NOT OPENSOURCE)
    RECURSE(
        analyze
        compare
    )
ENDIF()
