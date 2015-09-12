#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/codec.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/jerasure.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lrc.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/reed_solomon.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/codec.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/jerasure.h
  ${CMAKE_CURRENT_LIST_DIR}/lrc.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/reed_solomon.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

