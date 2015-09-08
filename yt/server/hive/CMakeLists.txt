#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/hive )
set( SRCS
  ${BASE}/commit.cpp
  ${BASE}/hive_manager.cpp
  ${BASE}/mailbox.cpp
  ${BASE}/public.cpp
  ${BASE}/helpers.cpp
  ${BASE}/transaction_supervisor.cpp
  ${BASE}/cell_directory_synchronizer.cpp
)
set( HDRS
  ${BASE}/commit.h
  ${BASE}/config.h
  ${BASE}/hive_manager.h
  ${BASE}/mailbox.h
  ${BASE}/public.h
  ${BASE}/helpers.h
  ${BASE}/transaction_manager.h
  ${BASE}/transaction_supervisor.h
  ${BASE}/cell_directory_synchronizer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/hive_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/transaction_supervisor.proto YT_SOURCE_FILES )
