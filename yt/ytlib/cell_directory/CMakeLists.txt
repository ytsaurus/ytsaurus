#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cell_directory )
set( SRCS
  ${BASE}/cell_directory.cpp
)
set( HDRS
  ${BASE}/cell_directory.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

