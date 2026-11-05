GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.14.0-rc.1)

SRCS(
    windowsbuilds.go
)

IF (OS_WINDOWS)
    SRCS(
        osversion_windows.go
        platform_compat_windows.go
    )

    GO_TEST_SRCS(
        osversion_windows_test.go
        platform_compat_windows_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
