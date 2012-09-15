#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cell_node )
set( SRCS
  ${BASE}/bootstrap.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/memory_consumer.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)