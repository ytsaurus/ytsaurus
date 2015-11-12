#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/distributed_election_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/election_manager_thunk.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/election_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/election_manager_mock.h
  ${CMAKE_CURRENT_LIST_DIR}/distributed_election_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/election_manager_thunk.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
