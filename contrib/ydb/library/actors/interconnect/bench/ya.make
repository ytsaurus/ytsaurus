PROGRAM(ic_bench)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/mock
    contrib/ydb/library/actors/interconnect/ut/lib
    contrib/ydb/library/actors/interconnect/ut/lib/port_manager
    contrib/ydb/library/actors/util
    library/cpp/getopt/small
    library/cpp/monlib/dynamic_counters
)

END()
