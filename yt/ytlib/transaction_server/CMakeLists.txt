#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/id.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_proxy.cpp
  ${BASE}/transaction_manager.cpp
  ${BASE}/transaction_service.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/id.h
  ${BASE}/cypress_integration.h
  ${BASE}/transaction.h
  ${BASE}/transaction_proxy.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_service.h
  ${BASE}/transaction_service_proxy.h
  ${BASE}/transaction_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_service.proto YT_SOURCE_FILES )
