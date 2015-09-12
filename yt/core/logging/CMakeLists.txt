#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/log.cpp
  ${CMAKE_CURRENT_LIST_DIR}/log_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pattern.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/writer.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/log-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/log.h
  ${CMAKE_CURRENT_LIST_DIR}/log_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/pattern.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
