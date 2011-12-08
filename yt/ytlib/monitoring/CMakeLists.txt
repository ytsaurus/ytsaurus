#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/monitoring )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/http_tree_server.cpp
  ${BASE}/monitoring_manager.cpp
  ${BASE}/ytree_integration.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/http_tree_server.h
  ${BASE}/monitoring_manager.h
  ${BASE}/ytree_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
