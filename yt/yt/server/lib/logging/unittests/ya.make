GTEST(unittester-library-logging)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    category_registry_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/server/lib/logging
    yt/yt/client
    yt/yt/core
)

SIZE(SMALL)

END()
