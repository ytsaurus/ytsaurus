#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/hive )
set( SRCS
  ${BASE}/commit.cpp
  ${BASE}/hive_manager.cpp
  ${BASE}/mailbox.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/transaction_supervisor.cpp
)
set( HDRS
  ${BASE}/commit.h
  ${BASE}/config.h
  ${BASE}/hive_manager.h
  ${BASE}/hive_service_proxy.h
  ${BASE}/mailbox.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_supervisor.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/hive_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/hive_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_supervisor.proto YT_SOURCE_FILES )
