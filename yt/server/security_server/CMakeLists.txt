#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/security_server )
set( SRCS
  ${BASE}/account.cpp
  ${BASE}/account_proxy.cpp
  ${BASE}/cluster_resources.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/private.cpp
  ${BASE}/security_manager.cpp
)
set( HDRS
  ${BASE}/account.h
  ${BASE}/account_proxy.h
  ${BASE}/cluster_resources.h
  ${BASE}/cypress_integration.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/security_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
