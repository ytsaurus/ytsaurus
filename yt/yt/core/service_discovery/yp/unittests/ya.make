GTEST(unittester-core-service-discovery-yp)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(g:yt)

IF (NOT OS_WINDOWS AND NOT ARCH_AARCH64)
    ALLOCATOR(YT)
ENDIF()

PROTO_NAMESPACE(yt)

SRCS(
    discovery_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/restrict_licenses_tests.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/service_discovery/yp
    yt/yt/core/test_framework
    library/cpp/testing/common
)

REQUIREMENTS(
    cpu:4
    ram:4
    ram_disk:1
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(MEDIUM)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:exotic_platform)
ENDIF()

END()
