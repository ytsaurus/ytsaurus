#:yt-updatabale
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/file_server )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/file_node.cpp
  ${BASE}/file_node_proxy.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/file_node.h
  ${BASE}/file_node_proxy.h
  ${BASE}/file_ypath_rpc.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/file_ypath_rpc.proto YT_SOURCE_FILES )
