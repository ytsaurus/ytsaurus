GO_PROGRAM()

LICENSE(Apache-2.0)

VERSION(v1.74.3)

IF (OS_LINUX)
    SRCS(
        client_linux.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        client_linux.go
    )
ENDIF()

END()
