set(NAME deps-arcadia-contrib-libs-base64)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/base64)
set(SRCS
  ${BASE}/neon32/codec_neon32.c
  ${BASE}/neon32/lib.c
  ${BASE}/neon64/codec_neon64.c
  ${BASE}/neon64/lib.c
  ${BASE}/plain32/codec_plain.c
  ${BASE}/plain32/lib.c
  ${BASE}/plain64/codec_plain.c
  ${BASE}/plain64/lib.c
)

add_library(${NAME}-avx2 STATIC ${BASE}/avx2/codec_avx2.c ${BASE}/avx2/lib.c)
add_library(${NAME}-ssse3 STATIC ${BASE}/ssse3/codec_ssse3.c ${BASE}/ssse3/lib.c)
add_library(${NAME} STATIC ${SRCS})
target_link_libraries(${NAME} ${NAME}-avx2 ${NAME}-ssse3)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set_target_properties(${NAME}-avx2 PROPERTIES COMPILE_FLAGS "-mavx2")
  set_target_properties(${NAME}-ssse3 PROPERTIES COMPILE_FLAGS "-mssse3")
else()
  message(FATAL "Unsupported toolchain. :(")
endif()

