#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/job_signal.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/job_prober_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/job_signal.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/job_prober_service.proto YT_SOURCE_FILES )
