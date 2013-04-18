#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/job_agent )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/job_controller.cpp
  ${BASE}/job.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/job_controller.h
  ${BASE}/job.h
  ${BASE}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
