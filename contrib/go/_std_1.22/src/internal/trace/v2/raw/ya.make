SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        doc.go
        event.go
        reader.go
        textreader.go
        textwriter.go
        writer.go
    )
ENDIF()
END()
