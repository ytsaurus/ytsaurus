set(NAME deps-arcadia-contrib-libs-base64)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/base64)
set(SRCS
  ${BASE}/avx2/codec_avx2.c
  ${BASE}/avx2/lib.c
  ${BASE}/ssse3/codec_ssse3.c
  ${BASE}/ssse3/lib.c
  ${BASE}/neon32/codec_neon32.c
  ${BASE}/neon32/lib.c
  ${BASE}/neon64/codec_neon64.c
  ${BASE}/neon64/lib.c
  ${BASE}/plain32/codec_plain.c
  ${BASE}/plain32/lib.c
  ${BASE}/plain64/codec_plain.c
  ${BASE}/plain64/lib.c
)

add_library(${NAME} STATIC ${SRCS})

if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set_target_properties(deps-arcadia-contrib-libs-base64 PROPERTIES COMPILE_FLAGS -mavx2)
  set_target_properties(deps-arcadia-contrib-libs-base64 PROPERTIES COMPILE_FLAGS -mssse3)
endif()

