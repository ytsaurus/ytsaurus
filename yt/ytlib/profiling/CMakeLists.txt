#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/profiling )
set( SRCS
  ${BASE}/profiler.cpp
  ${BASE}/profiling_manager.cpp
  ${BASE}/resource_tracker.cpp
  ${BASE}/timing.cpp
)
set( HDRS
  ${BASE}/profiler.h
  ${BASE}/profiling_manager.h
  ${BASE}/public.h
  ${BASE}/resource_tracker.h
  ${BASE}/timing.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
