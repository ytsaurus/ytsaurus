#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/attribute_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/attribute_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/attributes.cpp
  ${CMAKE_CURRENT_LIST_DIR}/convert.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ephemeral_attribute_owner.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ephemeral_node_factory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/exception_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/permission.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/system_attribute_provider.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tree_builder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tree_visitor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/virtual.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ypath_client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ypath_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ypath_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson_file_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson_serializable.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/attribute_consumer.h
  ${CMAKE_CURRENT_LIST_DIR}/attribute_helpers-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/attribute_helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/attribute_owner.h
  ${CMAKE_CURRENT_LIST_DIR}/attributes.h
  ${CMAKE_CURRENT_LIST_DIR}/convert-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/convert.h
  ${CMAKE_CURRENT_LIST_DIR}/ephemeral_attribute_owner.h
  ${CMAKE_CURRENT_LIST_DIR}/ephemeral_node_factory.h
  ${CMAKE_CURRENT_LIST_DIR}/exception_helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/fluent.h
  ${CMAKE_CURRENT_LIST_DIR}/node.h
  ${CMAKE_CURRENT_LIST_DIR}/node_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/permission.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/serialize-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/serialize.h
  ${CMAKE_CURRENT_LIST_DIR}/system_attribute_provider.h
  ${CMAKE_CURRENT_LIST_DIR}/tree_builder.h
  ${CMAKE_CURRENT_LIST_DIR}/tree_visitor.h
  ${CMAKE_CURRENT_LIST_DIR}/virtual.h
  ${CMAKE_CURRENT_LIST_DIR}/ypath_client-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/ypath_client.h
  ${CMAKE_CURRENT_LIST_DIR}/ypath_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/ypath_service.h
  ${CMAKE_CURRENT_LIST_DIR}/yson_file_service.h
  ${CMAKE_CURRENT_LIST_DIR}/yson_serializable-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/yson_serializable.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/attributes.proto YT_SOURCE_FILES )
