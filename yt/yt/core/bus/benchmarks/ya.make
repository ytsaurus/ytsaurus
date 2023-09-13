G_BENCHMARK()

OWNER(g:yt)

IF (NOT OS_WINDOWS AND NOT ARCH_AARCH64)
    ALLOCATOR(YT)
ENDIF()

SRCS(
    bus_bench.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

REQUIREMENTS(
    ram:16
)

END()
