#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/new_table_client )
set( SRCS
  ${BASE}/block_reader.cpp
  ${BASE}/block_writer.cpp
  ${BASE}/chunk_reader.cpp
  ${BASE}/chunk_writer.cpp
  ${BASE}/name_table.cpp
  ${BASE}/private.cpp
  ${BASE}/schema.cpp
)

set( HDRS
  ${BASE}/block_reader.h
  ${BASE}/block_writer.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_reader.h
  ${BASE}/chunk_writer.h
  ${BASE}/config.h
  ${BASE}/name_table.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/row.h
  ${BASE}/reader.h
  ${BASE}/schema.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_meta.proto YT_SOURCE_FILES )
