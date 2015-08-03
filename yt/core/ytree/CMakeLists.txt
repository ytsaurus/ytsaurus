#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/ytree )
set( SRCS
  ${BASE}/attribute_consumer.cpp
  ${BASE}/attribute_helpers.cpp
  ${BASE}/attributes.cpp
  ${BASE}/convert.cpp
  ${BASE}/ephemeral_attribute_owner.cpp
  ${BASE}/ephemeral_node_factory.cpp
  ${BASE}/exception_helpers.cpp
  ${BASE}/node.cpp
  ${BASE}/node_detail.cpp
  ${BASE}/permission.cpp
  ${BASE}/serialize.cpp
  ${BASE}/system_attribute_provider.cpp
  ${BASE}/tree_builder.cpp
  ${BASE}/tree_visitor.cpp
  ${BASE}/virtual.cpp
  ${BASE}/ypath_client.cpp
  ${BASE}/ypath_detail.cpp
  ${BASE}/ypath_service.cpp
  ${BASE}/yson_file_service.cpp
  ${BASE}/yson_serializable.cpp
)
set( HDRS
  ${BASE}/attribute_consumer.h
  ${BASE}/attribute_helpers-inl.h
  ${BASE}/attribute_helpers.h
  ${BASE}/attribute_owner.h
  ${BASE}/attributes.h
  ${BASE}/convert-inl.h
  ${BASE}/convert.h
  ${BASE}/ephemeral_attribute_owner.h
  ${BASE}/ephemeral_node_factory.h
  ${BASE}/exception_helpers.h
  ${BASE}/fluent.h
  ${BASE}/node.h
  ${BASE}/node_detail.h
  ${BASE}/permission.h
  ${BASE}/public.h
  ${BASE}/serialize-inl.h
  ${BASE}/serialize.h
  ${BASE}/system_attribute_provider.h
  ${BASE}/tree_builder.h
  ${BASE}/tree_visitor.h
  ${BASE}/virtual.h
  ${BASE}/ypath_client-inl.h
  ${BASE}/ypath_client.h
  ${BASE}/ypath_detail.h
  ${BASE}/ypath_proxy.h
  ${BASE}/ypath_service.h
  ${BASE}/yson_file_service.h
  ${BASE}/yson_serializable-inl.h
  ${BASE}/yson_serializable.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/attributes.proto YT_SOURCE_FILES )
