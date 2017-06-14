set(NAME deps-arcadia-contrib-libs-double--conversion)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion)
set(SRCS
  ${BASE}/cached-powers.cc
  ${BASE}/bignum-dtoa.cc
  ${BASE}/double-conversion.cc
  ${BASE}/diy-fp.cc
  ${BASE}/fixed-dtoa.cc
  ${BASE}/strtod.cc
  ${BASE}/bignum.cc
  ${BASE}/fast-dtoa.cc
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${BASE}
)

add_library(${NAME} STATIC ${SRCS})
