#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bootstrap.cpp
  ${CMAKE_CURRENT_LIST_DIR}/batching_chunk_service.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/bootstrap.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/batching_chunk_service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
