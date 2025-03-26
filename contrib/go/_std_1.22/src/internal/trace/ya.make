SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        gc.go
        goroutines.go
        mud.go
        order.go
        parser.go
        summary.go
        writer.go
    )
ENDIF()
END()
