PROGRAM(event_serialize_bench)

SRCS(
    bench.proto
    main.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/dnsresolver
    contrib/ydb/library/actors/interconnect
    library/cpp/monlib/dynamic_counters
)

END()
