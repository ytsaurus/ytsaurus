#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cypress_server )
set( SRCS
  ${BASE}/access_tracker.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/cypress_manager.cpp
  ${BASE}/cypress_traversing.cpp
  ${BASE}/helpers.cpp
  ${BASE}/lock.cpp
  ${BASE}/lock_proxy.cpp
  ${BASE}/node.cpp
  ${BASE}/node_detail.cpp
  ${BASE}/node_proxy.cpp
  ${BASE}/node_proxy_detail.cpp
  ${BASE}/private.cpp
  ${BASE}/virtual.cpp
)
set( HDRS
  ${BASE}/access_tracker.h
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/cypress_manager.h
  ${BASE}/cypress_traversing.h
  ${BASE}/helpers.h
  ${BASE}/lock.h
  ${BASE}/lock_proxy.h
  ${BASE}/node.h
  ${BASE}/node_detail.h
  ${BASE}/node_proxy.h
  ${BASE}/node_proxy_detail.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/type_handler.h
  ${BASE}/virtual-inl.h
  ${BASE}/virtual.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/cypress_manager.proto YT_SOURCE_FILES )
