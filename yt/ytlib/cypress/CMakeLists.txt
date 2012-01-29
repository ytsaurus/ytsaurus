#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/cypress_manager.cpp
  ${BASE}/cypress_service.cpp
  ${BASE}/cypress_service_proxy.cpp
  ${BASE}/id.cpp
  ${BASE}/lock.cpp
  ${BASE}/node.cpp
  ${BASE}/node_detail.cpp
  ${BASE}/node_proxy.cpp
  ${BASE}/node_proxy_detail.cpp
  ${BASE}/virtual.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/cypress_integration.h
  ${BASE}/cypress_manager.h
  ${BASE}/cypress_service.h
  ${BASE}/cypress_service_proxy-inl.h
  ${BASE}/cypress_service_proxy.h
  ${BASE}/cypress_ypath_proxy.h
  ${BASE}/id.h
  ${BASE}/lock.h
  ${BASE}/node.h
  ${BASE}/node_detail.h
  ${BASE}/node_proxy.h
  ${BASE}/node_proxy_detail.h
  ${BASE}/type_handler.h
  ${BASE}/virtual.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/cypress_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/cypress_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/cypress_manager.proto YT_SOURCE_FILES )
