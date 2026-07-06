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
)

END()
