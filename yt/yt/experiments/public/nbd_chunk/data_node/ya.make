PROGRAM()

ALLOCATOR(TCMALLOC)

SRCS(
    bootstrap.cpp
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/server/lib
    yt/yt/server/node
    yt/yt/library/program
    yt/yt/ytlib
)

END()
