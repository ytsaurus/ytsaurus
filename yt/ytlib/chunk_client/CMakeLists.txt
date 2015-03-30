#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client )
set( SRCS
  ${BASE}/block_id.cpp
  ${BASE}/helpers.cpp
  ${BASE}/chunk_meta_extensions.cpp
  ${BASE}/chunk_replica.cpp
  ${BASE}/chunk_slice.cpp
  ${BASE}/chunk_spec.cpp
  ${BASE}/client_block_cache.cpp
  ${BASE}/data_statistics.cpp
  ${BASE}/dispatcher.cpp
  ${BASE}/encoding_chunk_writer.cpp
  ${BASE}/encoding_writer.cpp
  ${BASE}/erasure_reader.cpp
  ${BASE}/erasure_writer.cpp
  ${BASE}/format.cpp
  ${BASE}/fetcher_base.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/memory_reader.cpp
  ${BASE}/memory_writer.cpp
  ${BASE}/multi_chunk_reader_base.cpp
  ${BASE}/multi_chunk_writer_base.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/read_limit.cpp
  ${BASE}/replication_reader.cpp
  ${BASE}/replication_writer.cpp
  ${BASE}/schema.cpp
  ${BASE}/sequential_reader.cpp
)
set( HDRS
  ${BASE}/chunk_reader.h
  ${BASE}/chunk_writer.h
  ${BASE}/block_cache.h
  ${BASE}/block_id.h
  ${BASE}/helpers.h
  ${BASE}/chunk_list_ypath_proxy.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_owner_ypath_proxy.h
  ${BASE}/chunk_reader_base.h
  ${BASE}/chunk_replica.h
  ${BASE}/chunk_service_proxy.h
  ${BASE}/chunk_slice.h
  ${BASE}/chunk_spec.h
  ${BASE}/chunk_writer_base.h
  ${BASE}/chunk_ypath_proxy.h
  ${BASE}/client_block_cache.h
  ${BASE}/config.h
  ${BASE}/data_node_service_proxy.h
  ${BASE}/data_statistics.h
  ${BASE}/dispatcher.h
  ${BASE}/encoding_chunk_writer.h
  ${BASE}/encoding_writer.h
  ${BASE}/erasure_reader.h
  ${BASE}/erasure_writer.h
  ${BASE}/fetcher_base.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer.h
  ${BASE}/format.h
  ${BASE}/memory_reader.h
  ${BASE}/memory_writer.h
  ${BASE}/multi_chunk_reader.h
  ${BASE}/multi_chunk_reader_base.h
  ${BASE}/multi_chunk_writer.h
  ${BASE}/multi_chunk_writer_base.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/reader_base.h
  ${BASE}/read_limit.h
  ${BASE}/replication_reader.h
  ${BASE}/replication_writer.h
  ${BASE}/schema.h
  ${BASE}/sequential_reader.h
  ${BASE}/writer_base.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_info.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_meta.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_spec.proto YT_SOURCE_FILES )
protoc( ${BASE}/schema.proto YT_SOURCE_FILES )
protoc( ${BASE}/job.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_owner_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/data_node_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_list_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/data_statistics.proto YT_SOURCE_FILES )
