GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.4-0)

IF (OS_LINUX OR OS_ANDROID)
    CGO_LDFLAGS(-Wl,--unresolved-symbols=ignore-in-object-files)
ENDIF()

CGO_CFLAGS(-DNVML_NO_UNVERSIONED_FUNC_DEFS=1)

GO_TEST_SRCS(
    # dl_test.go
)

IF (CGO_ENABLED)
    CGO_SRCS(dl.go)
ENDIF()

IF (OS_LINUX AND CGO_ENABLED)
    CGO_SRCS(dl_linux.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        dl_other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        dl_other.go
    )
ENDIF()

IF (OS_ANDROID AND CGO_ENABLED)
    CGO_SRCS(dl_linux.go)
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        dl_other.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
