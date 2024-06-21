GO_LIBRARY()

LICENSE(Apache-2.0)

IF (OS_LINUX)
    SRCS(
        appdefaults_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        appdefaults_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        appdefaults_windows.go
    )
ENDIF()

END()
