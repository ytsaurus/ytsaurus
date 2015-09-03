#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/job_tracker_client )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/statistics.cpp
)
set( HDRS
  ${BASE}/job_tracker_service_proxy.h
  ${BASE}/public.h
  ${BASE}/statistics-inl.h
  ${BASE}/statistics.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/job.proto YT_SOURCE_FILES )
protoc( ${BASE}/job_tracker_service.proto YT_SOURCE_FILES )
