PROGRAM(changelog_surgeon)

ALLOCATOR(YT)

PROTO_NAMESPACE(yt)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
    yt/yt/server/lib/io
    yt/yt/server/lib/hydra
    library/cpp/getopt/small
)

END()
