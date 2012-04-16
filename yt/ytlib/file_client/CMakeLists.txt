#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_reader_base.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/file_writer_base.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/file_reader.h
  ${BASE}/file_reader_base.h
  ${BASE}/file_writer.h
  ${BASE}/file_writer_base.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

