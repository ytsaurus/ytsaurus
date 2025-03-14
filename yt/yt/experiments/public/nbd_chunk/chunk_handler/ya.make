PROGRAM()

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/server/lib/nbd
    yt/yt/library/program
    yt/yt/ytlib
)

END()
