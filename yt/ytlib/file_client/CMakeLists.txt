#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/file_reader_base.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer_base.cpp
  ${BASE}/file_writer.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/file_reader_base.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer_base.h
  ${BASE}/file_writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/file_chunk_meta.proto YT_SOURCE_FILES )

