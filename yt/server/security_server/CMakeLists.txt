#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/account.cpp
  ${CMAKE_CURRENT_LIST_DIR}/account_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/acl.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cluster_resources.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/group.cpp
  ${CMAKE_CURRENT_LIST_DIR}/group_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/request_tracker.cpp
  ${CMAKE_CURRENT_LIST_DIR}/security_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/subject.cpp
  ${CMAKE_CURRENT_LIST_DIR}/subject_proxy_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/user.cpp
  ${CMAKE_CURRENT_LIST_DIR}/user_proxy.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/account.h
  ${CMAKE_CURRENT_LIST_DIR}/account_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/acl.h
  ${CMAKE_CURRENT_LIST_DIR}/cluster_resources.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.h
  ${CMAKE_CURRENT_LIST_DIR}/group.h
  ${CMAKE_CURRENT_LIST_DIR}/group_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/request_tracker.h
  ${CMAKE_CURRENT_LIST_DIR}/security_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/subject.h
  ${CMAKE_CURRENT_LIST_DIR}/subject_proxy_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/user.h
  ${CMAKE_CURRENT_LIST_DIR}/user_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/security_manager.proto YT_SOURCE_FILES )
