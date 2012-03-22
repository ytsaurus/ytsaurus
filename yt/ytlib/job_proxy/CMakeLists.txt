#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/job_proxy )
set( SRCS
#  ${BASE}/common.cpp
#  ${BASE}/job_proxy.cpp
#  ${BASE}/job_spec.cpp
#  ${BASE}/pipes.cpp
)
set( HDRS
#  ${BASE}/common.h
  ${BASE}/public.h
  ${BASE}/config.h
#  ${BASE}/job_proxy.h
#  ${BASE}/job_spec.h
#  ${BASE}/pipes.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

#PROTOC( ${BASE}/job_stats.proto YT_SOURCE_FILES )