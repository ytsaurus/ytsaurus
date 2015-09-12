#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/job.cpp
  ${CMAKE_CURRENT_LIST_DIR}/job_controller.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/job.h
  ${CMAKE_CURRENT_LIST_DIR}/job_controller.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
