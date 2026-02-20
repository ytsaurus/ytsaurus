GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.6.0)

SRCS(
    doc.go
)

IF (OS_LINUX)
    SRCS(
        sequential_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        sequential_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sequential_windows.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        sequential_unix.go
    )
ENDIF()

END()
