GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

IF (OS_WINDOWS)
    SRCS(
        grantvmgroupaccess.go
        syscall_windows.go
        zsyscall_windows.go
    )

    GO_TEST_SRCS(grantvmgroupaccess_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
