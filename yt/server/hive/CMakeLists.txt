#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/hive )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/hive_manager.cpp
  ${BASE}/mailbox.cpp
  ${BASE}/transaction_supervisor.cpp
  ${BASE}/commit.cpp
  ${BASE}/timestamp_manager.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/hive_manager.h
  ${BASE}/mailbox.h
  ${BASE}/hive_service_proxy.h
  ${BASE}/config.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_supervisor.h
  ${BASE}/commit.h
  ${BASE}/timestamp_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/hive_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/hive_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_supervisor.proto YT_SOURCE_FILES )
