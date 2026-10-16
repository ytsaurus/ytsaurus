LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/retro_tracing/collector
    contrib/ydb/library/actors/retro_tracing/span
)

END()

RECURSE(
    collector
    span
)

RECURSE_FOR_TESTS(
    ut
)
