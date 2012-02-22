#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/profiling )
set( SRCS
  ${BASE}/profiler.cpp
  ${BASE}/profiling_manager.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/profiler.h
  ${BASE}/profiling_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
