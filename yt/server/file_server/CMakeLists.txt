#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/file_server )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/file_node.cpp
  ${BASE}/file_node_proxy.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/file_node.h
  ${BASE}/file_node_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
