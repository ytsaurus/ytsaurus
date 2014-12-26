#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client )
set( SRCS
  ${BASE}/buffered_table_writer.cpp
  ${BASE}/channel_reader.cpp
  ${BASE}/channel_writer.cpp
  ${BASE}/chunk_writer_base.cpp
  ${BASE}/chunk_meta_extensions.cpp
  ${BASE}/partition_chunk_reader.cpp
  ${BASE}/partition_chunk_writer.cpp
  ${BASE}/partitioner.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/table_chunk_reader.cpp
  ${BASE}/table_chunk_writer.cpp
  ${BASE}/table_consumer.cpp
  ${BASE}/table_producer.cpp
  ${BASE}/table_reader.cpp
  ${BASE}/table_writer.cpp
  ${BASE}/value.cpp
)

set( HDRS
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/buffered_table_writer.h
  ${BASE}/channel_reader.h
  ${BASE}/channel_writer.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_writer_base.h
  ${BASE}/config.h
  ${BASE}/partition_chunk_reader.h
  ${BASE}/partition_chunk_writer.h
  ${BASE}/partitioner.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/sync_reader.h
  ${BASE}/sync_writer.h
  ${BASE}/table_chunk_reader.h
  ${BASE}/table_chunk_writer.h
  ${BASE}/table_consumer.h
  ${BASE}/table_producer.h
  ${BASE}/table_reader.h
  ${BASE}/table_writer.h
  ${BASE}/table_ypath_proxy.h
  ${BASE}/value.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/table_chunk_meta.proto YT_SOURCE_FILES )
protoc( ${BASE}/table_ypath.proto YT_SOURCE_FILES )
