#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/logging )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/log.cpp
  ${BASE}/log_manager.cpp
  ${BASE}/pattern.cpp
  ${BASE}/tagged_logger.cpp
  ${BASE}/writer.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/log.h
  ${BASE}/log_manager.h
  ${BASE}/pattern.h
  ${BASE}/tagged_logger.h
  ${BASE}/writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
