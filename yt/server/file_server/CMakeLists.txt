#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/file_node.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_node_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/file_node.h
  ${CMAKE_CURRENT_LIST_DIR}/file_node_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
