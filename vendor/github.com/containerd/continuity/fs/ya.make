GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    copy.go
    diff.go
    dir.go
    du.go
    hardlink.go
    path.go
    time.go
)

GO_TEST_SRCS(
    # copy_test.go
    # diff_test.go
    dir_test.go
    # dtype_test.go
    # du_test.go
)

IF (OS_LINUX)
    SRCS(
        copy_irregular_unix.go
        copy_linux.go
        copy_nondarwin.go
        diff_unix.go
        dtype_linux.go
        du_unix.go
        hardlink_unix.go
        stat_atim.go
    )

    GO_TEST_SRCS(
        # copy_linux_test.go
        # copy_unix_test.go
        # dtype_linux_test.go
        # du_cmd_unix_test.go
        # du_unix_test.go
        # path_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        copy_darwin.go
        copy_irregular_unix.go
        copy_unix.go
        diff_unix.go
        du_unix.go
        hardlink_unix.go
        stat_darwinbsd.go
        utimesnanoat.go
    )

    GO_TEST_SRCS(
        # copy_unix_test.go
        # du_cmd_freebsddarwin_test.go
        # du_unix_test.go
        # path_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        copy_nondarwin.go
        copy_windows.go
        diff_windows.go
        du_windows.go
        hardlink_windows.go
    )

    GO_TEST_SRCS(
        # du_windows_test.go
    )
ENDIF()

END()

RECURSE(
    fstest
    gotest
)
