#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/hive )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/hive_manager.cpp
  ${BASE}/mailbox.cpp
  ${BASE}/cell_directory.cpp
  ${BASE}/transaction_supervisor.cpp
  ${BASE}/distributed_commit.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/hive_manager.h
  ${BASE}/mailbox.h
  ${BASE}/hive_service_proxy.h
  ${BASE}/config.h
  ${BASE}/cell_directory.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_supervisor.h
  ${BASE}/distributed_commit.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/hive_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/hive_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_supervisor.proto YT_SOURCE_FILES )
