GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.12.4-0)

CGO_LDFLAGS(-Wl,--unresolved-symbols=ignore-in-object-files)

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

IF (OS_ANDROID AND CGO_ENABLED)
    CGO_SRCS(dl_linux.go)
ENDIF()

END()

RECURSE(
    gotest
)
