#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/build_attributes.cpp
  ${CMAKE_CURRENT_LIST_DIR}/fork_executor.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/build_attributes.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/memory_usage_tracker-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/memory_usage_tracker.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/fork_executor.h
  ${CMAKE_CURRENT_LIST_DIR}/object_helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
