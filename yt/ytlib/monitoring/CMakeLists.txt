#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/monitoring )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/http_integration.cpp
  ${BASE}/http_server.cpp
  ${BASE}/monitoring_manager.cpp
  ${BASE}/stat.cpp
  ${BASE}/statlog.cpp
  ${BASE}/ytree_integration.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/http_integration.h
  ${BASE}/http_server.h
  ${BASE}/monitoring_manager.h
  ${BASE}/stat-inl.h
  ${BASE}/stat.h
  ${BASE}/statlog.h
  ${BASE}/ytree_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
