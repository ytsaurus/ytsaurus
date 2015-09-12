#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_node.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_node_proxy.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/table_node.h
  ${CMAKE_CURRENT_LIST_DIR}/table_node_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
