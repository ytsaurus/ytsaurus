#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/file_server )
set( SRCS
  ${BASE}/file_node.cpp
  ${BASE}/file_node_proxy.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/file_node.h
  ${BASE}/file_node_proxy.h
  ${BASE}/private.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
