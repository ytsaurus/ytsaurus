GO_PROGRAM()

LICENSE(MIT)

VERSION(v0.6.2)

IF (OS_LINUX)
    SRCS(
        main_others.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        main_others.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        main_windows.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        main_others.go
    )
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        main_others.go
    )
ENDIF()

END()
