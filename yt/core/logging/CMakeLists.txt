#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/logging )
set( SRCS
  ${BASE}/config.cpp
  ${BASE}/common.cpp
  ${BASE}/log.cpp
  ${BASE}/log_manager.cpp
  ${BASE}/pattern.cpp
  ${BASE}/private.cpp
  ${BASE}/writer.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/config.h
  ${BASE}/log.h
  ${BASE}/log_manager.h
  ${BASE}/pattern.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
