#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/node_tracker_server )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/node.cpp
  ${BASE}/node_tracker.cpp
  ${BASE}/node_tracker_service.cpp
  ${BASE}/cypress_integration.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/node.h
  ${BASE}/node_authority.h
  ${BASE}/node_tracker.h
  ${BASE}/node_tracker_service.h
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/node_tracker.proto YT_SOURCE_FILES )
