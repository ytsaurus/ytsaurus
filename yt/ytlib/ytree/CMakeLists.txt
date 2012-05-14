#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/ytree )
set( SRCS
  ${BASE}/attribute_provider_detail.cpp
  ${BASE}/attributes.cpp
  ${BASE}/ephemeral.cpp
  ${BASE}/forwarding_yson_consumer.cpp
  ${BASE}/json_adapter.cpp
  ${BASE}/lexer.cpp
  ${BASE}/node_detail.cpp
  ${BASE}/null_yson_consumer.cpp
  ${BASE}/serialize.cpp
  ${BASE}/token.cpp
  ${BASE}/tokenizer.cpp
  ${BASE}/tree_builder.cpp
  ${BASE}/tree_visitor.cpp
  ${BASE}/virtual.cpp
  ${BASE}/ypath_client.cpp
  ${BASE}/ypath_detail.cpp
  ${BASE}/ypath_service.cpp
  ${BASE}/yson_consumer.cpp
  ${BASE}/yson_file_service.cpp
  ${BASE}/yson_parser.cpp
  ${BASE}/yson_writer.cpp
  ${BASE}/ytree.cpp
)
set( HDRS
  ${BASE}/attribute_provider.h
  ${BASE}/attribute_provider_detail.h
  ${BASE}/attributes-inl.h
  ${BASE}/attributes.h
  ${BASE}/ephemeral.h
  ${BASE}/fluent.h
  ${BASE}/forwarding_yson_consumer.h
  ${BASE}/json_adapter.h
  ${BASE}/lexer.h
  ${BASE}/node_detail.h
  ${BASE}/null_yson_consumer.h
  ${BASE}/public.h
  ${BASE}/serialize-inl.h
  ${BASE}/serialize.h
  ${BASE}/token.h
  ${BASE}/tokenizer.h
  ${BASE}/tree_builder.h
  ${BASE}/tree_visitor.h
  ${BASE}/virtual.h
  ${BASE}/ypath_client-inl.h
  ${BASE}/ypath_client.h
  ${BASE}/ypath_detail.h
  ${BASE}/ypath_format.h
  ${BASE}/ypath_proxy.h
  ${BASE}/ypath_service.h
  ${BASE}/yson_consumer-mock.h
  ${BASE}/yson_consumer.h
  ${BASE}/yson_file_service.h
  ${BASE}/yson_format.h
  ${BASE}/yson_parser.h
  ${BASE}/yson_writer.h
  ${BASE}/ytree.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/attributes.proto YT_SOURCE_FILES )
