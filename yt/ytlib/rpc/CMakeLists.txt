SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/common.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/client.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/channel.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/channel_cache.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/service.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/server.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/server_detail.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/message.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/retry.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc/rpc.proto YT_SOURCE_FILES )
