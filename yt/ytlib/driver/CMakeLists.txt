#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/driver )
set( SRCS
  ${BASE}/command.cpp
  ${BASE}/cypress_commands.cpp
  ${BASE}/dispatcher.cpp
  ${BASE}/driver.cpp
  ${BASE}/etc_commands.cpp
  ${BASE}/file_commands.cpp
  ${BASE}/private.cpp
  ${BASE}/query_callbacks_provider.cpp
  ${BASE}/scheduler_commands.cpp
  ${BASE}/table_commands.cpp
  ${BASE}/transaction_commands.cpp
  ${BASE}/table_mount_cache.cpp
)
set( HDRS
  ${BASE}/command.h
  ${BASE}/config.h
  ${BASE}/cypress_commands.h
  ${BASE}/dispatcher.cpp
  ${BASE}/driver.h
  ${BASE}/etc_commands.h
  ${BASE}/file_commands.h
  ${BASE}/private.h
  ${BASE}/query_callbacks_provider.h
  ${BASE}/public.h
  ${BASE}/scheduler_commands.h
  ${BASE}/table_commands.h
  ${BASE}/transaction_commands.h
  ${BASE}/table_mount_cache.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
