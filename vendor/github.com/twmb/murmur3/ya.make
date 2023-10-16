GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    murmur.go
    murmur128.go
    murmur32.go
    murmur32_gen.go
    murmur64.go
)

GO_TEST_SRCS(murmur_test.go)

IF (ARCH_X86_64)
    SRCS(
        murmur128_amd64.s
        murmur128_decl.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(murmur128_gen.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(murmur128_gen.go)
ENDIF()

END()

IF (OS_LINUX)
    RECURSE(
        testdata
        gotest
    )
ENDIF()
