#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client )
set( SRCS
  ${BASE}/file_chunk_output.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_reader_base.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/file_chunk_output.h
  ${BASE}/file_reader.h
  ${BASE}/file_reader_base.h
  ${BASE}/file_writer.h
  ${BASE}/file_ypath_proxy.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/file_ypath.proto YT_SOURCE_FILES )
