SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_node.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_node_proxy.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_manager.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_service_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_service.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_service_rpc.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_manager.proto YT_SOURCE_FILES )
