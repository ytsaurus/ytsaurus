SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ytree_fwd.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ytree.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/yson_format.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/yson_events.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/forwarding_yson_events.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/null_yson_consumer.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/yson_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/yson_writer.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/tree_builder.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/tree_visitor.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/node_detail.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ephemeral.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/fluent.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/json_adapter.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/virtual.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ypath_client.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ypath_service.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ypath_detail.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ypath_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/yson_file_service.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree/ypath_rpc.proto YT_SOURCE_FILES )
