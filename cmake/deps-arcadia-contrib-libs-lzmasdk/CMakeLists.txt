set(NAME deps-arcadia-contrib-libs-lzmasdk)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/lzmasdk)
set(SRCS
  ${BASE}/7zStream.c
  ${BASE}/Alloc.c
  ${BASE}/LzmaDec.c
  ${BASE}/LzmaEnc.c
  ${BASE}/LzFind.c
  ${BASE}/LzmaLib.c
  ${BASE}/LzmaUtil.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

include_directories(${CMAKE_SOURCE_DIR})

add_definitions(-D_7ZIP_ST=1)

add_library(${NAME} STATIC ${SRCS})

if (_is_gcc)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -Wno-error)
endif()
