#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/transaction_server )
set( SRCS
  ${BASE}/cypress_integration.cpp
  ${BASE}/private.cpp
  ${BASE}/timestamp_manager.cpp
  ${BASE}/timestamp_proxy_service.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_manager.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/timestamp_manager.h
  ${BASE}/timestamp_proxy_service.h
  ${BASE}/transaction.h
  ${BASE}/transaction_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/timestamp_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_manager.proto YT_SOURCE_FILES )
