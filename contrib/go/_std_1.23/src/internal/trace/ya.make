SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        base.go
        batch.go
        batchcursor.go
        event.go
        gc.go
        generation.go
        mud.go
        oldtrace.go
        order.go
        parser.go
        reader.go
        resources.go
        summary.go
        value.go
    )
ENDIF()
END()
