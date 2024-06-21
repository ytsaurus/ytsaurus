GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    defaults.go
    doc.go
)

IF (OS_LINUX)
    SRCS(
        defaults_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        defaults_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        defaults_windows.go
    )
ENDIF()

END()
