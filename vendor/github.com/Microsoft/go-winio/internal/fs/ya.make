GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    doc.go
    security.go
)

IF (OS_WINDOWS)
    SRCS(
        fs.go
        zsyscall_windows.go
    )

    GO_TEST_SRCS(fs_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
