GO_PROGRAM()

SRCS(
    main.go
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        test_medium
    )
ENDIF()
