#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_manager.cpp
  ${BASE}/transaction_service.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/cypress_integration.h
  ${BASE}/transaction.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_service.h
  ${BASE}/transaction_service_rpc.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_service_rpc.proto YT_SOURCE_FILES )
