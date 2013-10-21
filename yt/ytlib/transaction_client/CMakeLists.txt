#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_client )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_listener.cpp
  ${BASE}/transaction_manager.cpp
  ${BASE}/transaction_ypath_proxy.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/transaction.h
  ${BASE}/transaction_listener.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_ypath.proto YT_SOURCE_FILES )
