#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cgroup )
set( SRCS
  ${BASE}/cgroup.cpp
)

set( HDRS
  ${BASE}/cgroup.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

