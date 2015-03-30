#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/node_tracker_server )
set( SRCS
  ${BASE}/cypress_integration.cpp
  ${BASE}/node.cpp
  ${BASE}/rack.cpp
  ${BASE}/rack_proxy.cpp
  ${BASE}/node_tracker.cpp
  ${BASE}/node_tracker_service.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/node_directory_builder.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/node.h
  ${BASE}/rack.h
  ${BASE}/rack_proxy.h
  ${BASE}/node_tracker.h
  ${BASE}/node_tracker_service.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/node_directory_builder.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/node_tracker.proto YT_SOURCE_FILES )
