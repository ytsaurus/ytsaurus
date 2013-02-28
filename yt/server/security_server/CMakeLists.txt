#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/security_server )
set( SRCS
  ${BASE}/acl.cpp
  ${BASE}/account.cpp
  ${BASE}/account_proxy.cpp
  ${BASE}/subject.cpp
  ${BASE}/user.cpp
  ${BASE}/user_proxy.cpp
  ${BASE}/group.cpp
  ${BASE}/group_proxy.cpp
  ${BASE}/subject_proxy_detail.cpp
  ${BASE}/cluster_resources.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/private.cpp
  ${BASE}/security_manager.cpp
)
set( HDRS
  ${BASE}/acl.h
  ${BASE}/account.h
  ${BASE}/account_proxy.h
  ${BASE}/subject.h
  ${BASE}/user.h
  ${BASE}/user_proxy.h
  ${BASE}/group.h
  ${BASE}/group_proxy.h
  ${BASE}/subject_proxy_detail.h
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
