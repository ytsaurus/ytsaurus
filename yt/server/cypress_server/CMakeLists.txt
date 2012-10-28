#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cypress_server )
set( SRCS
  ${BASE}/cluster_resources.cpp
  ${BASE}/cypress_manager.cpp
  ${BASE}/helpers.cpp
  ${BASE}/lock.cpp
  ${BASE}/node.cpp
  ${BASE}/node_detail.cpp
  ${BASE}/node_proxy.cpp
  ${BASE}/node_proxy_detail.cpp
  ${BASE}/virtual.cpp
)
set( HDRS
  ${BASE}/cluster_resources.h
  ${BASE}/cypress_manager.h
  ${BASE}/helpers.h
  ${BASE}/lock.h
  ${BASE}/node.h
  ${BASE}/node_detail.h
  ${BASE}/node_proxy.h
  ${BASE}/node_proxy_detail.h
  ${BASE}/public.h
  ${BASE}/type_handler.h
  ${BASE}/virtual.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
