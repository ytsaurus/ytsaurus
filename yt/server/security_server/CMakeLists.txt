#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/security_server )
set( SRCS
  ${BASE}/account.cpp
  ${BASE}/account_proxy.cpp
  ${BASE}/acl.cpp
  ${BASE}/cluster_resources.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/group.cpp
  ${BASE}/group_proxy.cpp
  ${BASE}/private.cpp
  ${BASE}/security_manager.cpp
  ${BASE}/subject.cpp
  ${BASE}/subject_proxy_detail.cpp
  ${BASE}/user.cpp
  ${BASE}/user_proxy.cpp
  ${BASE}/request_tracker.cpp
)
set( HDRS
  ${BASE}/account.h
  ${BASE}/account_proxy.h
  ${BASE}/acl.h
  ${BASE}/cluster_resources.h
  ${BASE}/cypress_integration.h
  ${BASE}/group.h
  ${BASE}/group_proxy.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/security_manager.h
  ${BASE}/subject.h
  ${BASE}/subject_proxy_detail.h
  ${BASE}/user.h
  ${BASE}/user_proxy.h
  ${BASE}/request_tracker.h
  ${BASE}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/security_manager.proto YT_SOURCE_FILES )
