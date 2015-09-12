#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/job_tracker_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/job.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/job_tracker_service.proto YT_SOURCE_FILES )
