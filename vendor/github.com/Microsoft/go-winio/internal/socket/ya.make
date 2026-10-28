GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    rawaddr.go
)

IF (OS_WINDOWS)
    SRCS(
        socket.go
        zsyscall_windows.go
    )
ENDIF()

END()
