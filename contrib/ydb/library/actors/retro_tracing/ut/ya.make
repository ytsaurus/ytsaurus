UNITTEST()

FORK_SUBTESTS()

SRCS(
    test_spans.cpp
    main.cpp
)

PEERDIR(
    contrib/ydb/library/actors/interconnect/retro_tracing
    contrib/ydb/library/actors/retro_tracing
    contrib/ydb/library/actors/testlib
)

END()
