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

if (NOT WIN32)
  list(APPEND YT_SOURCE_FILES
    ${BASE}/fiber-supp.S
  )
  
  set_source_files_properties(
    ${BASE}/fiber-supp.S
    PROPERTIES LANGUAGE C
  )
endif()

PUMP( ${BASE}/coroutine.h.pump YT_SOURCE_FILES )
