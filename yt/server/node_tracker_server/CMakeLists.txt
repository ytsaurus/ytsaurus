#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_directory_builder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rack.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rack_proxy.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.h
  ${CMAKE_CURRENT_LIST_DIR}/node.h
  ${CMAKE_CURRENT_LIST_DIR}/node_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/node_directory_builder.h
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker.h
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_service.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/rack.h
  ${CMAKE_CURRENT_LIST_DIR}/rack_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/node_tracker.proto YT_SOURCE_FILES )
