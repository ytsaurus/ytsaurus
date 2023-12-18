PROGRAM(benchmark_queues)

ALLOCATOR(YT)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/program
    library/cpp/getopt/small
)

END()
