UNITTEST_FOR(contrib/ydb/library/actors/wilson)

SRCS(
    wilson_span_ut.cpp
    wilson_trace_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/testlib
)

END()
