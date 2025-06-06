GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.33.0)

SRCS(
    byteorder.go
    cpu.go
    endian_little.go
    parse.go
    runtime_auxv.go
    runtime_auxv_go121.go
)

GO_TEST_SRCS(
    parse_test.go
    runtime_auxv_go121_test.go
)

GO_XTEST_SRCS(
    cpu_test.go
    endian_test.go
)

IF (ARCH_X86_64)
    SRCS(
        cpu_gc_x86.go
        cpu_gc_x86.s
        cpu_x86.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        cpu_arm64.go
        cpu_arm64.s
        cpu_gc_arm64.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        hwcap_linux.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        cpu_linux_noinit.go
        cpu_other_x86.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        cpu_linux_arm64.go
        proc_cpuinfo_linux.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM6 OR OS_LINUX AND ARCH_ARM7)
    SRCS(
        cpu_arm.go
        cpu_linux.go
        cpu_linux_arm.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
        asm_darwin_x86_gc.s
        cpu_darwin_x86.go
        syscall_darwin_x86_gc.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        cpu_other_arm64.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
        cpu_other_x86.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        cpu_other_arm64.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
