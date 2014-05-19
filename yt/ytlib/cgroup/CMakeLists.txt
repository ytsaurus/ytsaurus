#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cgroup )
set( SRCS
  ${BASE}/cgroup.cpp
  ${BASE}/statistics.cpp
  ${BASE}/private.cpp
)

set( HDRS
  ${BASE}/cgroup.h
  ${BASE}/statistics.h
  ${BASE}/private.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/statistics.proto YT_SOURCE_FILES )
