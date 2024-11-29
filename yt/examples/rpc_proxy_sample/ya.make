PROGRAM(rpc_proxy_example)

ALLOCATOR(TCMALLOC)

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

