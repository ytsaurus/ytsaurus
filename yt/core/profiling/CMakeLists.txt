#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/profiling )
set( SRCS
  ${BASE}/profiler.cpp
  ${BASE}/profile_manager.cpp
  ${BASE}/public.cpp
  ${BASE}/resource_tracker.cpp
  ${BASE}/timing.cpp
)
set( HDRS
  ${BASE}/profiler.h
  ${BASE}/profile_manager.h
  ${BASE}/public.h
  ${BASE}/resource_tracker.h
  ${BASE}/scoped_timer.h
  ${BASE}/timing.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
