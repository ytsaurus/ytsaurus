GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
)

IF (OS_WINDOWS)
    SRCS(
        ansi_reader.go
        ansi_writer.go
        console.go
    )

    GO_TEST_SRCS(ansi_reader_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
