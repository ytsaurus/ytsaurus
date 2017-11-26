set(NAME deps-arcadia-contrib-libs-cctz)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/cctz)
set(SRCS
  ${BASE}/src/civil_time_detail.cc
  ${BASE}/src/time_zone_fixed.cc
  ${BASE}/src/time_zone_format.cc
  ${BASE}/src/time_zone_if.cc
  ${BASE}/src/time_zone_impl.cc
  ${BASE}/src/time_zone_info.cc
  ${BASE}/src/time_zone_libc.cc
  ${BASE}/src/time_zone_lookup.cc
  ${BASE}/src/time_zone_posix.cc
)

include_directories(
  ${BASE}/include
)

add_library(${NAME} STATIC ${SRCS})

