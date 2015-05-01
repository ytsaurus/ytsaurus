#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/pipes )
set( SRCS
  ${BASE}/async_reader.cpp
  ${BASE}/async_writer.cpp
  ${BASE}/io_dispatcher.cpp
  ${BASE}/io_dispatcher_impl.cpp
  ${BASE}/private.cpp
  ${BASE}/pipe.cpp

)
set( HDRS
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/io_dispatcher.h
  ${BASE}/io_dispatcher_impl.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/pipe.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
