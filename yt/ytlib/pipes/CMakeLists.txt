#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/pipes )
set( SRCS
  ${BASE}/io_dispatcher.cpp
  ${BASE}/io_dispatcher_impl.cpp
  ${BASE}/async_reader.cpp
  ${BASE}/async_writer.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/io_dispatcher.h
  ${BASE}/io_dispatcher_impl.h
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
