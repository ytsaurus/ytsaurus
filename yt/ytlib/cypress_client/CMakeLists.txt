#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress_client )
set( SRCS
  ${BASE}/cypress_ypath_proxy.cpp
  ${BASE}/public.cpp
  ${BASE}/rpc_helpers.cpp
)
set( HDRS
  ${BASE}/cypress_ypath_proxy.h
  ${BASE}/lock_ypath_proxy.h
  ${BASE}/public.h
  ${BASE}/rpc_helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/rpc.proto YT_SOURCE_FILES )
protoc( ${BASE}/cypress_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/lock_ypath.proto YT_SOURCE_FILES )
