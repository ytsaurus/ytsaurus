#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/new_table_client )
set( SRCS
  ${BASE}/name_table.cpp
  ${BASE}/private.cpp
  ${BASE}/schema.cpp
  ${BASE}/schemed_block_reader.cpp
  ${BASE}/schemed_block_writer.cpp
  ${BASE}/schemed_chunk_reader.cpp
  ${BASE}/schemed_chunk_writer.cpp
  ${BASE}/unversioned_row.cpp
  ${BASE}/versioned_block_reader.cpp
  ${BASE}/versioned_block_writer.cpp
  ${BASE}/versioned_chunk_reader.cpp
  ${BASE}/versioned_chunk_writer.cpp
  ${BASE}/versioned_multi_chunk_writer.cpp
  ${BASE}/versioned_row.cpp
  ${BASE}/cached_versioned_chunk_meta.cpp
  ${BASE}/pipe.cpp
  ${BASE}/row_buffer.cpp
)

set( HDRS
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/config.h
  ${BASE}/name_table.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/row_base.h
  ${BASE}/reader.h
  ${BASE}/schema.h
  ${BASE}/schemed_block_reader.h
  ${BASE}/schemed_block_writer.h
  ${BASE}/schemed_chunk_reader.h
  ${BASE}/schemed_chunk_writer.h
  ${BASE}/schemed_reader.h
  ${BASE}/schemed_writer.h
  ${BASE}/unversioned_row.h
  ${BASE}/versioned_block_writer.h
  ${BASE}/versioned_block_reader.h
  ${BASE}/versioned_chunk_reader.h
  ${BASE}/versioned_chunk_writer.h
  ${BASE}/versioned_multi_chunk_writer.h
  ${BASE}/versioned_reader.h
  ${BASE}/versioned_row.h
  ${BASE}/versioned_writer.h
  ${BASE}/writer.h
  ${BASE}/unversioned_writer.h
  ${BASE}/cached_versioned_chunk_meta.h
  ${BASE}/pipe.h
  ${BASE}/row_buffer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_meta.proto YT_SOURCE_FILES )
