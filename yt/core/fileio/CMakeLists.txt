#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/fileio )
set( SRCS
  ${BASE}/file_io_dispatcher.cpp
  ${BASE}/file_io_dispatcher_impl.cpp
  ${BASE}/async_reader.cpp
)
set( HDRS
  ${BASE}/file_io_dispatcher.h
  ${BASE}/file_io_dispatcher_impl.h
  ${BASE}/async_reader.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
