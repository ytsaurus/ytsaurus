#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/new_table_client )
set( SRCS
  ${BASE}/cached_versioned_chunk_meta.cpp
  ${BASE}/chunk_splits_fetcher.cpp
  ${BASE}/chunk_writer_base.cpp
  ${BASE}/name_table.cpp
  ${BASE}/pipe.cpp
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/row_base.cpp
  ${BASE}/row_buffer.cpp
  ${BASE}/samples_fetcher.cpp
  ${BASE}/schema.cpp
  ${BASE}/schemaful_block_reader.cpp
  ${BASE}/schemaful_block_writer.cpp
  ${BASE}/schemaful_chunk_reader.cpp
  ${BASE}/schemaful_chunk_writer.cpp
  ${BASE}/schemaless_block_writer.cpp
  ${BASE}/schemaless_chunk_writer.cpp
  ${BASE}/schemaful_merging_reader.cpp
  ${BASE}/table_producer.cpp
  ${BASE}/unversioned_row.cpp
  ${BASE}/versioned_block_reader.cpp
  ${BASE}/versioned_block_writer.cpp
  ${BASE}/versioned_chunk_reader.cpp
  ${BASE}/versioned_chunk_writer.cpp
  ${BASE}/versioned_row.cpp
  ${BASE}/versioned_chunk_lookuper.cpp
)

set( HDRS
  ${BASE}/block_writer.h
  ${BASE}/cached_versioned_chunk_meta.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_splits_fetcher.h
  ${BASE}/chunk_writer_base.h
  ${BASE}/config.h
  ${BASE}/name_table.h
  ${BASE}/pipe.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/row_base.h
  ${BASE}/row_buffer.h
  ${BASE}/samples_fetcher.h
  ${BASE}/schema.h
  ${BASE}/schemaful_block_reader.h
  ${BASE}/schemaful_block_writer.h
  ${BASE}/schemaful_chunk_reader.h
  ${BASE}/schemaful_chunk_writer.h
  ${BASE}/schemaful_reader.h
  ${BASE}/schemaful_writer.h
  ${BASE}/schemaless_block_writer.h
  ${BASE}/schemaless_chunk_writer.h
  ${BASE}/schemaless_writer.h
  ${BASE}/schemaful_merging_reader.h
  ${BASE}/table_producer.h
  ${BASE}/unversioned_row.h
  ${BASE}/unversioned_writer.h
  ${BASE}/versioned_block_writer.h
  ${BASE}/versioned_block_reader.h
  ${BASE}/versioned_chunk_reader.h
  ${BASE}/versioned_chunk_writer.h
  ${BASE}/versioned_reader.h
  ${BASE}/versioned_row.h
  ${BASE}/versioned_writer.h
  ${BASE}/writer.h
  ${BASE}/versioned_lookuper.h
  ${BASE}/versioned_chunk_lookuper.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_meta.proto YT_SOURCE_FILES )
