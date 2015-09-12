#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/commit.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hive_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/mailbox.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_supervisor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cell_directory_synchronizer.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/commit.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/hive_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/mailbox.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_supervisor.h
  ${CMAKE_CURRENT_LIST_DIR}/cell_directory_synchronizer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/hive_manager.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/transaction_supervisor.proto YT_SOURCE_FILES )
