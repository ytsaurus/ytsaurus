#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/fibers )
set( SRCS
  ${BASE}/coroutine.cpp
  ${BASE}/fiber.cpp
)
set( HDRS
  ${BASE}/fiber-inl.h
  ${BASE}/fiber.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

PUMP( ${BASE}/coroutine.h.pump YT_SOURCE_FILES )
