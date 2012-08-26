#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cypress_server )
set( SRCS
  ${BASE}/cypress_manager.cpp
  ${BASE}/node.cpp
  ${BASE}/node_detail.cpp
  ${BASE}/node_proxy.cpp
  ${BASE}/node_proxy_detail.cpp
  ${BASE}/virtual.cpp
  ${BASE}/lock.cpp
)
set( HDRS
  ${BASE}/cypress_manager.h
  ${BASE}/node.h
  ${BASE}/node_detail.h
  ${BASE}/node_proxy.h
  ${BASE}/node_proxy_detail.h
  ${BASE}/public.h
  ${BASE}/type_handler.h
  ${BASE}/virtual.h
  ${BASE}/lock.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
