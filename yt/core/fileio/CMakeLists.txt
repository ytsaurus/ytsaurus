#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/fileio )
set( SRCS
  ${BASE}/file_io_dispatcher.cpp
  ${BASE}/file_io_dispatcher_impl.cpp
  ${BASE}/async_reader.cpp
  ${BASE}/async_writer.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/file_io_dispatcher.h
  ${BASE}/file_io_dispatcher_impl.h
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/private.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
