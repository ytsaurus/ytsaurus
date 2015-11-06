#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/tclap_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/workload.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/tclap_helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/workload.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/workload.proto YT_SOURCE_FILES )
