GTEST(unittester-core-dump)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    core_dump_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/library/sparse_coredump
)

SIZE(MEDIUM)

END()
