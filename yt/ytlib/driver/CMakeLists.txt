#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/driver )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/driver.cpp
  ${BASE}/command.cpp
  ${BASE}/cypress_commands.cpp
  ${BASE}/transaction_commands.cpp
  ${BASE}/file_commands.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/driver.h
  ${BASE}/command.h
  ${BASE}/cypress_commands.h
  ${BASE}/transaction_commands.h
  ${BASE}/file_commands.h
  ${BASE}/table_commands.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
