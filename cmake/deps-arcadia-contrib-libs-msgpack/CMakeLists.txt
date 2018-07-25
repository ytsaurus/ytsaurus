set(NAME deps-arcadia-contrib-libs-msgpack)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/msgpack)
set(SRCS
  ${BASE}/src/objectc.c
  ${BASE}/src/unpack.c
  ${BASE}/src/version.c
  ${BASE}/src/vrefbuffer.c
  ${BASE}/src/zone.c
)

add_library(${NAME} STATIC ${SRCS})

target_include_directories(${NAME}
  PUBLIC
  ${BASE}/include
)
