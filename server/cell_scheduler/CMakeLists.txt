#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cell_scheduler )
set( SRCS
  ${BASE}/bootstrap.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)