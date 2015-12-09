set(NAME deps-arcadia-contrib-libs-snappy)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/snappy)
set(SRCS
  ${BASE}/snappy.cc
  ${BASE}/snappy-sinksource.cc
  ${BASE}/snappy-stubs-internal.cc
  ${BASE}/snappy-c.cc
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

