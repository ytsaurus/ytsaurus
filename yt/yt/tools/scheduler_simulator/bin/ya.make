PROGRAM(scheduler_simulator)

ALLOCATOR(YT)

SRCS(
    main.cpp
)

ADDINCL(
    yt/yt/tools/scheduler_simulator
)

PEERDIR(
    yt/yt/tools/scheduler_simulator
    library/cpp/yt/phdr_cache
    library/cpp/getopt
)

END()
