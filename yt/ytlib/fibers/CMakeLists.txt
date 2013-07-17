#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/fibers )
set( SRCS
  ${BASE}/fiber.cpp
  ${BASE}/fiber-supp.S
  ${BASE}/coroutine.cpp
)
set( HDRS
  ${BASE}/fiber.h
  ${BASE}/fiber-inl.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

set_source_files_properties( ${BASE}/fiber-supp.S PROPERTIES LANGUAGE C )

PUMP( ${BASE}/coroutine.h.pump YT_SOURCE_FILES )
