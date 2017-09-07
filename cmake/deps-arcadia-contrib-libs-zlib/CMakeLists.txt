set(NAME deps-arcadia-contrib-libs-zlib)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/zlib)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error")

set(SRCS
  ${BASE}/adler32.c
  ${BASE}/compress.c
  ${BASE}/crc32.c
  ${BASE}/deflate.c
  ${BASE}/gzclose.c
  ${BASE}/gzlib.c
  ${BASE}/gzread.c
  ${BASE}/gzwrite.c
  ${BASE}/infback.c
  ${BASE}/inffast.c
  ${BASE}/inflate.c
  ${BASE}/inftrees.c
  ${BASE}/trees.c
  ${BASE}/uncompr.c
  ${BASE}/zutil.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_definitions(-DBUILD_ZLIB -DY_BUILD_AS_ORIGIN)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})
