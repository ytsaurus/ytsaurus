GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

IF (OS_WINDOWS)
    SRCS(
        bind_filter.go
        zsyscall_windows.go
    )

    GO_TEST_SRCS(bind_filter_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
