#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/job_proxy )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/job_proxy.cpp
  ${BASE}/pipes.cpp
  ${BASE}/user_job.cpp
  ${BASE}/merge_job.cpp
  ${BASE}/user_job_io.cpp
  ${BASE}/map_job_io.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/config.h
  ${BASE}/job_proxy.h
  ${BASE}/pipes.h
  ${BASE}/job.h
  ${BASE}/user_job.h
  ${BASE}/merge_job.h
  ${BASE}/user_job_io.h
  ${BASE}/map_job_io.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

#PROTOC( ${BASE}/job_stats.proto YT_SOURCE_FILES )