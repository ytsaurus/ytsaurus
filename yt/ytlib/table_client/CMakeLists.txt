#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client )
set( SRCS
  ${BASE}/channel_reader.cpp
  ${BASE}/channel_writer.cpp
  ${BASE}/chunk_reader.cpp
  ${BASE}/chunk_sequence_reader.cpp
  ${BASE}/chunk_sequence_writer.cpp
  ${BASE}/chunk_writer.cpp
  ${BASE}/common.cpp
  ${BASE}/schema.cpp
  ${BASE}/table_reader.cpp
  ${BASE}/table_writer.cpp
  ${BASE}/value.cpp
  ${BASE}/validating_writer.cpp
  ${BASE}/sorted_validating_writer.cpp
)
set( HDRS
  ${BASE}/channel_reader.h
  ${BASE}/channel_writer.h
  ${BASE}/chunk_reader.h
  ${BASE}/chunk_sequence_reader.h
  ${BASE}/chunk_sequence_writer.h
  ${BASE}/chunk_writer.h
  ${BASE}/common.h
  ${BASE}/reader.h
  ${BASE}/schema.h
  ${BASE}/table_reader.h
  ${BASE}/table_writer.h
  ${BASE}/value.h
  ${BASE}/validating_writer.h
  ${BASE}/sorted_validating_writer.h
  ${BASE}/async_writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/table_chunk_meta.proto YT_SOURCE_FILES )
protoc( ${BASE}/schema.proto YT_SOURCE_FILES )
protoc( ${BASE}/table_reader.proto YT_SOURCE_FILES )
