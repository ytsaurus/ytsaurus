#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cgroup.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/config.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/cgroup.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
