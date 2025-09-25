GO_PROGRAM()

SRCS(
    main.go
)

IF (OPENSOURCE)
    SRCS(
        maxprocs_external.go
    )
ELSE()
    SRCS(
        maxprocs_internal.go
    )
ENDIF()

END()
