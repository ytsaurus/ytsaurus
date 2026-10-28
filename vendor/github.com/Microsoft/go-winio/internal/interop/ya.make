GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    doc.go
)

IF (OS_WINDOWS)
    SRCS(
        interop.go
        zsyscall_windows.go
    )
ENDIF()

END()
