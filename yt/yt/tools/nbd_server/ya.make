PROGRAM()

CMAKE_EXPORTED_TARGET_NAME(nbd_server_tool)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/core/service_discovery/yp
    yt/yt/ytlib
    yt/yt/library/program
    yt/yt/library/query/engine
    yt/yt/server/lib/nbd
    yt/yt/server/lib/nbd/chunk
    yt/yt/server/lib/nbd/dynamic_table
    yt/yt/server/lib/nbd/image
    yt/yt/server/lib/nbd/memory
)

END()
