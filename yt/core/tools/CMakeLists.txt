#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/tools.cpp
  ${CMAKE_CURRENT_LIST_DIR}/registry.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/tools.h
  ${CMAKE_CURRENT_LIST_DIR}/tools-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/registry.h
  ${CMAKE_CURRENT_LIST_DIR}/registry-inl.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
