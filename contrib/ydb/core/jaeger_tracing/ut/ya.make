UNITTEST_FOR(contrib/ydb/core/jaeger_tracing)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    sampler_ut.cpp
    throttler_ut.cpp
)

END()
