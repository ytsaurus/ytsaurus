GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    doc.go
)

IF (OS_WINDOWS)
    SRCS(
        strconv.go
        tar.go
    )

    GO_TEST_SRCS(tar_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
