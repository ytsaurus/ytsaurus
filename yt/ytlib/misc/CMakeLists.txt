#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/misc )
set( SRCS
  ${BASE}/tclap_helpers.cpp
  ${BASE}/workload.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/tclap_helpers.h
  ${BASE}/workload.h
  ${BASE}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/workload.proto YT_SOURCE_FILES )
