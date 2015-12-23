#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/table_server )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/table_node.cpp
  ${BASE}/table_node_proxy.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/table_node.h
  ${BASE}/table_node_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
