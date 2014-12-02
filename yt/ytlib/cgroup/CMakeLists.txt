#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cgroup )
set( SRCS
  ${BASE}/cgroup.cpp
  ${BASE}/event.cpp
  ${BASE}/private.cpp
)

set( HDRS
  ${BASE}/cgroup.h
  ${BASE}/event.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
