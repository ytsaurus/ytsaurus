GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    doc.go
)

IF (OS_WINDOWS)
    SRCS(
        fs_windows.go
        resolve.go
    )

    GO_TEST_SRCS(
        fs_windows_test.go
        resolve_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
