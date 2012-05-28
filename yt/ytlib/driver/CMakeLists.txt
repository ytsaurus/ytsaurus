#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/driver )
set( SRCS
  ${BASE}/command.cpp
  ${BASE}/cypress_commands.cpp
  ${BASE}/driver.cpp
  ${BASE}/file_commands.cpp
  ${BASE}/format.cpp
  ${BASE}/private.cpp
  ${BASE}/scheduler_commands.cpp
  ${BASE}/table_commands.cpp
  ${BASE}/transaction_commands.cpp
  ${BASE}/tsv_parser.cpp
  ${BASE}/tsv_writer.cpp
)
set( HDRS
  ${BASE}/command.h
  ${BASE}/config.h
  ${BASE}/cypress_commands.h
  ${BASE}/driver.h
  ${BASE}/file_commands.h
  ${BASE}/format.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/scheduler_commands.h
  ${BASE}/table_commands.h
  ${BASE}/transaction_commands.h
  ${BASE}/tsv_parser.h
  ${BASE}/tsv_writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
