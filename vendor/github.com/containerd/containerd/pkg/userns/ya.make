GO_LIBRARY()

LICENSE(Apache-2.0)

IF (OS_LINUX)
    SRCS(
        userns_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        userns_unsupported.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        userns_unsupported.go
    )
ENDIF()

END()
