GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    internal.go
)

IF (OS_LINUX)
    SRCS(
        debug_linux.go
        unix.go
        unix2.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        darwin.go
        debug_darwin.go
        debug_kqueue.go
        unix2.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        debug_windows.go
        windows.go
    )
ENDIF()

END()
