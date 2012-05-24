#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/driver )
set( SRCS
  ${BASE}/command.cpp
  ${BASE}/private.cpp
  ${BASE}/cypress_commands.cpp
  ${BASE}/driver.cpp
  ${BASE}/format.cpp
  ${BASE}/cypress_commands.cpp
  ${BASE}/file_commands.cpp
  ${BASE}/scheduler_commands.cpp
  ${BASE}/table_commands.cpp
  ${BASE}/transaction_commands.cpp
)
set( HDRS
  ${BASE}/command.h
  ${BASE}/config.h
  ${BASE}/driver.h
  ${BASE}/format.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/cypress_commands.h
  ${BASE}/file_commands.h
  ${BASE}/scheduler_commands.h
  ${BASE}/table_commands.h
  ${BASE}/transaction_commands.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
