set( BASE ${CMAKE_SOURCE_DIR}/contrib/libcoro )

add_library( ytext-coro STATIC
  ${BASE}/coro.c
)

