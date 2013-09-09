#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/ypath )
set( SRCS
  ${BASE}/rich.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/rich.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
