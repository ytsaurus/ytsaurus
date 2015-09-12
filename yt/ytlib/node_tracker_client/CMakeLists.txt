#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_directory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_directory_builder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_statistics.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/node_directory.h
  ${CMAKE_CURRENT_LIST_DIR}/node_directory_builder.h
  ${CMAKE_CURRENT_LIST_DIR}/node_statistics.h
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/node.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/node_tracker_service.proto YT_SOURCE_FILES )
