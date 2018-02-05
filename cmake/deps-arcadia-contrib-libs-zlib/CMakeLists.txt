set(NAME deps-arcadia-contrib-libs-zlib)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/zlib)

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

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

if (_is_gcc)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -Wno-error)
endif()
