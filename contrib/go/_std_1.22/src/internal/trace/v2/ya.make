SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        base.go
        batch.go
        batchcursor.go
        event.go
        generation.go
        order.go
        reader.go
        resources.go
        value.go
    )
ENDIF()
END()
