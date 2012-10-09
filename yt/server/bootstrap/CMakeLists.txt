#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/bootstrap )
set( SRCS
  ${BASE}/common.cpp
)
set( HDRS
  ${BASE}/common.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
