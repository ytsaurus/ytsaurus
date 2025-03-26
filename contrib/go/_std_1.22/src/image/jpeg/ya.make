SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        fdct.go
        huffman.go
        idct.go
        reader.go
        scan.go
        writer.go
    )
ENDIF()
END()
