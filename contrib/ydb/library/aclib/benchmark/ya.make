G_BENCHMARK(library_aclib_benchmark)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
SIZE(LARGE)

SRCS(
    b_aclib.cpp
)

PEERDIR(
    contrib/ydb/library/aclib
)

END()
