#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/table_server )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/table_node.cpp
  ${BASE}/table_node_proxy.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/table_node.h
  ${BASE}/table_node_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
