#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/node_tracker_client )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/node_directory.cpp
  ${BASE}/node_directory_builder.cpp
  ${BASE}/node_statistics.cpp
  ${BASE}/helpers.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/node_directory.h
  ${BASE}/node_directory_builder.h
  ${BASE}/node_tracker_service_proxy.h
  ${BASE}/node_statistics.h
  ${BASE}/helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/node.proto YT_SOURCE_FILES )
protoc( ${BASE}/node_tracker_service.proto YT_SOURCE_FILES )
