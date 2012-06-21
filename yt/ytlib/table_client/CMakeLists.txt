#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client )
set( SRCS
  ${BASE}/channel_reader.cpp
  ${BASE}/channel_writer.cpp
  ${BASE}/chunk_reader.cpp
  ${BASE}/chunk_writer_base.cpp
  ${BASE}/chunk_sequence_reader.cpp
  ${BASE}/key.cpp
  ${BASE}/partition_chunk_sequence_writer.cpp
  ${BASE}/partition_chunk_writer.cpp
  ${BASE}/private.cpp
  ${BASE}/schema.cpp
  ${BASE}/size_limits.cpp
  ${BASE}/sync_reader.cpp
  ${BASE}/table_chunk_sequence_writer.cpp
  ${BASE}/table_chunk_writer.cpp
  ${BASE}/table_consumer.cpp
  ${BASE}/table_producer.cpp
  ${BASE}/table_reader.cpp
  ${BASE}/table_writer.cpp
  ${BASE}/value.cpp
  ${BASE}/merging_reader.cpp
)

set( HDRS
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/channel_reader.h
  ${BASE}/channel_writer.h
  ${BASE}/chunk_writer_base.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_reader.h
  ${BASE}/chunk_sequence_reader.h
  ${BASE}/chunk_sequence_writer_base-inl.h
  ${BASE}/chunk_sequence_writer_base.h
  ${BASE}/config.h
  ${BASE}/key.h
  ${BASE}/partition_chunk_sequence_writer.h
  ${BASE}/partition_chunk_writer.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/schema.h
  ${BASE}/size_limits.h
  ${BASE}/sync_reader.h
  ${BASE}/sync_writer.h
  ${BASE}/table_chunk_sequence_writer.h
  ${BASE}/table_chunk_writer.h
  ${BASE}/table_consumer.h
  ${BASE}/table_producer.h
  ${BASE}/table_reader.h
  ${BASE}/table_writer.h
  ${BASE}/validating_writer.h
  ${BASE}/value.h
  ${BASE}/merging_reader.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/table_chunk_meta.proto YT_SOURCE_FILES )
protoc( ${BASE}/table_reader.proto YT_SOURCE_FILES )
