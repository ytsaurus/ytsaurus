#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/scheduler )
set( SRCS
  ${BASE}/redirector_service.cpp
)
set( HDRS
  ${BASE}/redirector_service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
