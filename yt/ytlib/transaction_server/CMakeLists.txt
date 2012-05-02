#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server )
set( SRCS
  ${BASE}/cypress_integration.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_manager.cpp
  ${BASE}/transaction_ypath_proxy.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/public.h
  ${BASE}/transaction.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_ypath.proto YT_SOURCE_FILES )
