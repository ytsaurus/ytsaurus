set( BASE ${CMAKE_SOURCE_DIR}/contrib/libcoro )

add_library( ytext-coro STATIC
  ${BASE}/coro.c
)

if (WIN32)
  add_definitions(-DCORO_FIBER)
endif()

