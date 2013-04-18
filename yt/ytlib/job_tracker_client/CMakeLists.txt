#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/job_tracker_client )
set( SRCS
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/job_tracker_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/job.proto YT_SOURCE_FILES )
protoc( ${BASE}/job_tracker_service.proto YT_SOURCE_FILES )
