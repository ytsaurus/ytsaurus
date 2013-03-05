#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client )
set( SRCS
  ${BASE}/block_id.cpp
  ${BASE}/chunk_meta_extensions.cpp
  ${BASE}/client_block_cache.cpp
  ${BASE}/dispatcher.cpp
  ${BASE}/encoding_writer.cpp
  ${BASE}/erasure_reader.cpp
  ${BASE}/erasure_writer.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/replication_reader.cpp
  ${BASE}/replication_writer.cpp
  ${BASE}/sequential_reader.cpp
  ${BASE}/chunk_replica.cpp
  ${BASE}/node_directory.cpp
  ${BASE}/node_directory_builder.cpp
)
set( HDRS
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/block_cache.h
  ${BASE}/block_id.h
  ${BASE}/chunk_list_ypath_proxy.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_ypath_proxy.h
  ${BASE}/client_block_cache.h
  ${BASE}/config.h
  ${BASE}/data_node_service_proxy.h
  ${BASE}/dispatcher.h
  ${BASE}/erasure_reader.h
  ${BASE}/encoding_writer.h
  ${BASE}/erasure_writer.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer.h
  ${BASE}/format.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/replication_reader.h
  ${BASE}/replication_writer.h
  ${BASE}/sequential_reader.h
  ${BASE}/chunk_replica.h
  ${BASE}/node_directory.h
  ${BASE}/node_directory_builder.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/node.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk.proto YT_SOURCE_FILES )
protoc( ${BASE}/data_node_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_list_ypath.proto YT_SOURCE_FILES )
