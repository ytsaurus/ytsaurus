GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    guid.go
    variant_string.go
)

GO_TEST_SRCS(guid_test.go)

IF (OS_LINUX)
    SRCS(
        guid_nonwindows.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        guid_nonwindows.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        guid_windows.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        guid_nonwindows.go
    )
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        guid_nonwindows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
