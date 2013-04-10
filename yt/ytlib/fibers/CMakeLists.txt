#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/fibers )
set( SRCS
  ${BASE}/fiber.cpp
  ${BASE}/coroutine.cpp
)
set( HDRS
  ${BASE}/fiber.h
  ${BASE}/coroutine.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

PUMP( ${BASE}/coroutine.h.pump YT_SOURCE_FILES )

