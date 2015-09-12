#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/command.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cypress_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver.cpp
  ${CMAKE_CURRENT_LIST_DIR}/etc_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/journal_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_commands.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/command.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_commands.h
  ${CMAKE_CURRENT_LIST_DIR}/dispatcher.h
  ${CMAKE_CURRENT_LIST_DIR}/driver.h
  ${CMAKE_CURRENT_LIST_DIR}/etc_commands.h
  ${CMAKE_CURRENT_LIST_DIR}/file_commands.h
  ${CMAKE_CURRENT_LIST_DIR}/journal_commands.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_commands.h
  ${CMAKE_CURRENT_LIST_DIR}/table_commands.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_commands.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
