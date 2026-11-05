GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.4-0)

IF (OS_LINUX OR OS_ANDROID)
    CGO_LDFLAGS(-Wl,--unresolved-symbols=ignore-in-object-files)
ENDIF()

CGO_CFLAGS(-DNVML_NO_UNVERSIONED_FUNC_DEFS=1)

SRCS(
    api.go
    const_static.go
    device.go
    doc.go
    dynamicLibrary_mock.go
    event_set.go
    gpm.go
    memory_limits.go
    refcount.go
    return.go
    system.go
    types_gen.go
    unit.go
    vgpu.go
    zz_generated.api.go
)

GO_TEST_SRCS(
    device_test.go
    gpm_test.go
    lib_test.go
    # nvml_test.go
    refcount_test.go
)

IF (CGO_ENABLED)
    CGO_SRCS(
        cgo_helpers_static.go
        const.go
        init.go
        lib.go
        nvml.go
    )
ENDIF()

END()

RECURSE(
    gotest
    mock
)
