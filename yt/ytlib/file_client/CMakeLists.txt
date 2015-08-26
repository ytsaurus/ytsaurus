#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client )
set( SRCS
  ${BASE}/file_chunk_output.cpp
  ${BASE}/file_chunk_reader.cpp
  ${BASE}/file_chunk_writer.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/config.h
  ${BASE}/file_chunk_output.h
  ${BASE}/file_chunk_reader.h
  ${BASE}/file_chunk_writer.h
  ${BASE}/file_ypath_proxy.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/file_chunk_meta.proto YT_SOURCE_FILES )

