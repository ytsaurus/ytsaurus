#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/statistics.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/job_prober_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/statistics-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/statistics.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/scheduler_service.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/job.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/job_prober_service.proto YT_SOURCE_FILES )
