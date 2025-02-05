LIBRARY()

SRCS(
    wilson_event.cpp
    wilson_span.cpp
    wilson_profile_span.cpp
    wilson_trace.cpp
    wilson_uploader.cpp
)

PEERDIR(
    contrib/libs/opentelemetry-proto
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    contrib/ydb/library/actors/wilson/protos
)

END()

RECURSE(
    protos
)

RECURSE_FOR_TESTS(
    ut
    test_util
)
