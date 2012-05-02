#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_client )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_listener.cpp
  ${BASE}/transaction_manager.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/config.h
  ${BASE}/public.h
  ${BASE}/transaction.h
  ${BASE}/transaction_listener.h
  ${BASE}/transaction_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
