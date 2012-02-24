#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cell_master )
set( SRCS
  ${BASE}/bootstrap.cpp
  ${BASE}/config.cpp
  ${BASE}/load_context.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/load_context.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)