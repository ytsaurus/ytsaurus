#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/http_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/http_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/monitoring_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/http_integration.h
  ${CMAKE_CURRENT_LIST_DIR}/http_server.h
  ${CMAKE_CURRENT_LIST_DIR}/monitoring_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
