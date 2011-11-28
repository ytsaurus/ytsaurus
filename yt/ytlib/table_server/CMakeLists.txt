SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_node.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_node_proxy.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_ypath_rpc.h
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server/table_ypath_rpc.proto YT_SOURCE_FILES )
