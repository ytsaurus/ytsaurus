#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cypress_executors.cpp
  ${CMAKE_CURRENT_LIST_DIR}/etc_executors.cpp
  ${CMAKE_CURRENT_LIST_DIR}/executor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_executors.cpp
  ${CMAKE_CURRENT_LIST_DIR}/io_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/journal_executors.cpp
  ${CMAKE_CURRENT_LIST_DIR}/main.cpp
  ${CMAKE_CURRENT_LIST_DIR}/operation_tracker.cpp
  ${CMAKE_CURRENT_LIST_DIR}/preprocess.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_executors.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_executors.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_executors.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_executors.h
  ${CMAKE_CURRENT_LIST_DIR}/etc_executors.h
  ${CMAKE_CURRENT_LIST_DIR}/executor.h
  ${CMAKE_CURRENT_LIST_DIR}/file_executors.h
  ${CMAKE_CURRENT_LIST_DIR}/io_helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/journal_executors.h
  ${CMAKE_CURRENT_LIST_DIR}/operation_tracker.h
  ${CMAKE_CURRENT_LIST_DIR}/preprocess.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_executors.h
  ${CMAKE_CURRENT_LIST_DIR}/table_executors.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_executors.h
)

add_executable( yt ${SRCS} ${HDRS} )

target_link_libraries( yt
  ytlib
  ytext-lfalloc
)

if (NOT WIN32)
  set_target_properties( yt
    PROPERTIES
    LINK_FLAGS "-static-libstdc++"
  )
endif()

set_target_properties( yt PROPERTIES
  LINK_FLAGS "-static-libstdc++"
)

install(
  TARGETS yt
  RUNTIME DESTINATION bin
)

