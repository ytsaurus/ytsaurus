#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/security_server )
set( SRCS
  ${BASE}/cluster_resources.cpp
  ${BASE}/account.cpp
  ${BASE}/account_proxy.cpp
  ${BASE}/security_manager.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/cluster_resources.h
  ${BASE}/account.h
  ${BASE}/account_proxy.h
  ${BASE}/security_manager.h
  ${BASE}/public.h
  ${BASE}/cypress_integration.h
  ${BASE}/private.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
