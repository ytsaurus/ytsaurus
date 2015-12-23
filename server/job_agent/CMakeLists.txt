#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/job_agent )
set( SRCS
  ${BASE}/job.cpp
  ${BASE}/job_controller.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/job.h
  ${BASE}/job_controller.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
