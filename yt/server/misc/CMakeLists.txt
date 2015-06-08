#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/misc )
set( SRCS
  ${BASE}/build_attributes.cpp
  ${BASE}/fork_snapshot_builder.cpp
)
set( HDRS
  ${BASE}/build_attributes.h
  ${BASE}/config.h
  ${BASE}/memory_usage_tracker-inl.h
  ${BASE}/memory_usage_tracker.h
  ${BASE}/public.h
  ${BASE}/fork_snapshot_builder.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
