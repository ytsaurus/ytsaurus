#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/orchid )
set( SRCS
  ${BASE}/cypress_integration.cpp
)
set( HDRS
  ${BASE}/cypress_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
