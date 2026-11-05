GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

IF (OS_WINDOWS)
    SRCS(
        process.go
        syscall.go
        zsyscall_windows.go
    )
ENDIF()

END()
