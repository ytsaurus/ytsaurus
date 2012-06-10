#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_client )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_listener.cpp
  ${BASE}/transaction_manager.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/config.h
  ${BASE}/transaction.h
  ${BASE}/transaction_listener.h
  ${BASE}/transaction_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
