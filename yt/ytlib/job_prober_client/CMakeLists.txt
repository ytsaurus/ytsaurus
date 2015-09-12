#:yt-updatable
set( SRCS
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/job_prober_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/job_prober_service.proto YT_SOURCE_FILES )
