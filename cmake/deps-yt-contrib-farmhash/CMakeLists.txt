set(NAME deps-yt-contrib-farmhash)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/farmhash)
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

if(MSVC)
  add_definitions(
    -DFARMHASH_NO_BUILTIN_EXPECT
  )
endif()

#include_directories(${BASE})

add_library(${NAME} STATIC ${SRCS})
