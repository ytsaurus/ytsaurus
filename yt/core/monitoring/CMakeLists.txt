#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/monitoring )
set( SRCS
  ${BASE}/http_integration.cpp
  ${BASE}/http_server.cpp
  ${BASE}/monitoring_manager.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/http_integration.h
  ${BASE}/http_server.h
  ${BASE}/monitoring_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
