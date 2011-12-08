#:yt-updatabale
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/cypress_manager.cpp
  ${BASE}/cypress_service.cpp
  ${BASE}/cypress_ypath_rpc.cpp
  ${BASE}/lock.cpp
  ${BASE}/node.cpp
  ${BASE}/node_detail.cpp
  ${BASE}/node_proxy.cpp
  ${BASE}/node_proxy_detail.cpp
  ${BASE}/virtual.cpp
  ${BASE}/world_initializer.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/cypress_integration.h
  ${BASE}/cypress_manager.h
  ${BASE}/cypress_service.h
  ${BASE}/cypress_service_rpc-inl.h
  ${BASE}/cypress_service_rpc.h
  ${BASE}/cypress_ypath_rpc.h
  ${BASE}/lock.h
  ${BASE}/node.h
  ${BASE}/node_detail.h
  ${BASE}/node_proxy.h
  ${BASE}/node_proxy_detail.h
  ${BASE}/virtual.h
  ${BASE}/world_initializer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/cypress_ypath_rpc.proto YT_SOURCE_FILES )
protoc( ${BASE}/cypress_service_rpc.proto YT_SOURCE_FILES )
protoc( ${BASE}/cypress_manager.proto YT_SOURCE_FILES )
