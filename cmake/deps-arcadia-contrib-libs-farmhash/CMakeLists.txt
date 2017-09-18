set(NAME deps-arcadia-contrib-libs-farmhash)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash)
set(SRCS
  ${BASE}/farmhash.cc
)

if(_vendor_id STREQUAL "GenuineIntel")
  add_definitions(
    -DFARMHASH_ASSUME_SSE3
    -DFARMHASH_ASSUME_SSE41
    -DFARMHASH_ASSUME_SSE42
  )
endif()

add_library(${NAME} STATIC ${SRCS})
