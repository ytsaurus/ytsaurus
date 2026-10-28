GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.3.0)

SRCS(
    oleutil.go
)

IF (OS_LINUX)
    SRCS(
        connection_func.go
        go-get.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        connection_func.go
        go-get.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        connection.go
        connection_windows.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        connection_func.go
        go-get.go
    )
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        connection_func.go
        go-get.go
    )
ENDIF()

END()
