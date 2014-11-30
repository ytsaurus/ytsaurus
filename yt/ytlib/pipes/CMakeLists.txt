#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/pipes )
set( SRCS
  ${BASE}/io_dispatcher.cpp
  ${BASE}/io_dispatcher_impl.cpp
  ${BASE}/nonblocking_reader.cpp
  ${BASE}/nonblocking_writer.cpp
  ${BASE}/async_io.cpp
  ${BASE}/async_reader.cpp
  ${BASE}/async_writer.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/io_dispatcher.h
  ${BASE}/io_dispatcher_impl.h
  ${BASE}/nonblocking_reader.h
  ${BASE}/nonblocking_writer.h
  ${BASE}/async_io.h
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
