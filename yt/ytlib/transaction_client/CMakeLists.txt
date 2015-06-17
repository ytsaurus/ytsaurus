#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_client )
set( SRCS
  ${BASE}/helpers.cpp
  ${BASE}/private.cpp
  ${BASE}/remote_timestamp_provider.cpp
  ${BASE}/transaction_listener.cpp
  ${BASE}/transaction_manager.cpp
  ${BASE}/transaction_ypath_proxy.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/helpers.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/remote_timestamp_provider.h
  ${BASE}/timestamp_provider.h
  ${BASE}/timestamp_service_proxy.h
  ${BASE}/transaction_listener.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/timestamp_service.proto YT_SOURCE_FILES )
