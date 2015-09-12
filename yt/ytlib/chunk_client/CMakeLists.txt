#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/block_id.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_meta_extensions.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_replica.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_scraper.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_slice.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_spec.cpp
  ${CMAKE_CURRENT_LIST_DIR}/client_block_cache.cpp
  ${CMAKE_CURRENT_LIST_DIR}/data_statistics.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/encoding_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/encoding_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/fetcher_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/format.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/memory_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/memory_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/multi_chunk_reader_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/multi_chunk_writer_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/read_limit.cpp
  ${CMAKE_CURRENT_LIST_DIR}/replication_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/replication_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schema.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sequential_reader.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/block_cache.h
  ${CMAKE_CURRENT_LIST_DIR}/block_id.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_list_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_meta_extensions.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_reader_base.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_replica.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_scraper.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_slice.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_spec.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_writer_base.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/client_block_cache.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/data_node_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/data_statistics.h
  ${CMAKE_CURRENT_LIST_DIR}/dispatcher.h
  ${CMAKE_CURRENT_LIST_DIR}/encoding_chunk_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/encoding_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/erasure_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/erasure_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/fetcher_base.h
  ${CMAKE_CURRENT_LIST_DIR}/file_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/file_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/format.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/memory_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/memory_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/multi_chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/multi_chunk_reader_base.h
  ${CMAKE_CURRENT_LIST_DIR}/multi_chunk_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/multi_chunk_writer_base.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/read_limit.h
  ${CMAKE_CURRENT_LIST_DIR}/reader_base.h
  ${CMAKE_CURRENT_LIST_DIR}/replication_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/replication_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/schema.h
  ${CMAKE_CURRENT_LIST_DIR}/sequential_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/writer_base.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_info.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_meta.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_spec.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/schema.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/job.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/data_node_service.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_service.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_list_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/data_statistics.proto YT_SOURCE_FILES )
