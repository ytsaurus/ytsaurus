#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cgroup )
set( SRCS
  ${BASE}/cgroup.cpp
  ${BASE}/private.cpp
  ${BASE}/config.cpp
)

set( HDRS
  ${BASE}/cgroup.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
