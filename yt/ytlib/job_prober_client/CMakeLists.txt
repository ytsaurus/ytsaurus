#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/job_prober_client )
set( SRCS
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/job_prober_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/job_prober_service.proto YT_SOURCE_FILES )
