#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/table_server )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/table_node.cpp
  ${BASE}/table_node_proxy.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/table_node.h
  ${BASE}/table_node_proxy.h
  ${BASE}/table_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/table_ypath.proto YT_SOURCE_FILES )
