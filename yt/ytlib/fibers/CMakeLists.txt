#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/fibers )
set( SRCS
  ${BASE}/fiber.cpp
)
set( HDRS
  ${BASE}/coroutine.h
  ${BASE}/fiber.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

