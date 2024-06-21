GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    run.go
    winsize.go
)

GO_TEST_SRCS(
    doc_test.go
    io_test.go
)

IF (ARCH_X86_64)
    SRCS(
        ztypes_amd64.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        ztypes_arm64.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        ioctl.go
        ioctl_inner.go
        pty_linux.go
        start.go
        winsize_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        ioctl.go
        ioctl_bsd.go
        ioctl_inner.go
        pty_darwin.go
        start.go
        winsize_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        pty_unsupported.go
        start_windows.go
        winsize_unsupported.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
