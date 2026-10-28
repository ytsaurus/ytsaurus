GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

IF (OS_LINUX)
    SRCS(
        decompress.go
        wim.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        decompress.go
        wim.go
    )
ENDIF()

IF (OS_ANDROID)
    SRCS(
        decompress.go
        wim.go
    )
ENDIF()

END()

RECURSE(
    lzx
)

IF (OS_LINUX)
    RECURSE(
        validate
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        validate
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        validate
    )
ENDIF()
