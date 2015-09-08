#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/journal_server )
set( SRCS
  ${BASE}/journal_node.cpp
  ${BASE}/journal_node_proxy.cpp
  ${BASE}/journal_manager.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/journal_node.h
  ${BASE}/journal_node_proxy.h
  ${BASE}/journal_manager.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
