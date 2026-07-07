GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.17.0)

SRCS(
    doc.go
    math.go
    signbit.go
)

IF (ARCH_X86_64)
    SRCS(
        sqrt_amd64.go
        sqrt_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        sqrt_arm64.go
        sqrt_arm64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM6 OR OS_LINUX AND ARCH_ARM7)
    SRCS(
        sqrt.go
    )
ENDIF()

IF (OS_EMSCRIPTEN)
    SRCS(
        sqrt.go
    )
ENDIF()

END()
