SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/file_node.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/file_node_proxy.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/file_manager.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/file_service_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/file_service.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/file_manager.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server/file_service_rpc.proto YT_SOURCE_FILES )
