GO_LIBRARY()

LICENSE(MPL-2.0)

VERSION(v0.6.6)

IF (OS_LINUX)
    SRCS(
        fs_default.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        fs_default.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        fs_windows.go
    )
ENDIF()

END()
