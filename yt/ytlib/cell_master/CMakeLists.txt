#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cell_master )
set( SRCS
  ${BASE}/bootstrap.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)