GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.4.4)

SRCS(
    devices.go
)

IF (OS_LINUX)
    SRCS(
        devices_unix.go
        mknod_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        devices_unix.go
        mknod_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        devices_windows.go
    )
ENDIF()

END()
