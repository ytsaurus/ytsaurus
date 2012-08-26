#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/transaction_server )
set( SRCS
  ${BASE}/cypress_integration.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_manager.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/public.h
  ${BASE}/transaction.h
  ${BASE}/transaction_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
