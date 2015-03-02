#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client )
set( SRCS
  ${BASE}/channel_reader.cpp
  ${BASE}/chunk_meta_extensions.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/table_chunk_reader.cpp
  ${BASE}/value.cpp
)

set( HDRS
  ${BASE}/channel_reader.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/config.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/table_chunk_reader.h
  ${BASE}/value.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/table_chunk_meta.proto YT_SOURCE_FILES )
