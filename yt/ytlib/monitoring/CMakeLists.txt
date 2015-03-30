#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/monitoring )
set( SRCS
  ${BASE}/http_integration.cpp
  ${BASE}/http_server.cpp
  ${BASE}/monitoring_manager.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/http_integration.h
  ${BASE}/http_server.h
  ${BASE}/monitoring_manager.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
