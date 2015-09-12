#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/admin.cpp
  ${CMAKE_CURRENT_LIST_DIR}/client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/connection.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/journal_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/journal_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rowset.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/admin.h
  ${CMAKE_CURRENT_LIST_DIR}/box.h
  ${CMAKE_CURRENT_LIST_DIR}/client.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/connection.h
  ${CMAKE_CURRENT_LIST_DIR}/file_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/file_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/journal_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/journal_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/rowset.h
  ${CMAKE_CURRENT_LIST_DIR}/table_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
