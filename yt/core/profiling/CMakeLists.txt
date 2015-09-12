#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/profile_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/profiler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/resource_tracker.cpp
  ${CMAKE_CURRENT_LIST_DIR}/timing.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/profile_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/profiler.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/resource_tracker.h
  ${CMAKE_CURRENT_LIST_DIR}/scoped_timer.h
  ${CMAKE_CURRENT_LIST_DIR}/timing.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
