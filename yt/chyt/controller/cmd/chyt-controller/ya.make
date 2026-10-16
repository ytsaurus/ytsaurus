GO_PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    helpers.go
    init_cluster.go
    main.go
    one_shot_run.go
    run.go
)

IF (OPENSOURCE)
    SRCS(
        dq_opensource.go
    )
ENDIF()

IF (NOT OPENSOURCE)
    SRCS(
        dq_internal.go
    )
ENDIF()

END()
