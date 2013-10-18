#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/new_table_client )
set( SRCS
  ${BASE}/name_table.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/row.h
  ${BASE}/schema.h
  ${BASE}/name_table.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_meta.proto YT_SOURCE_FILES )

