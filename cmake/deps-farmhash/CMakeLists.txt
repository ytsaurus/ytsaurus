set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash )

set( SRCS_FARMHASH
  ${BASE}/farmhash.cc
)

add_library( farmhash STATIC
  ${SRCS_FARMHASH}
)

if(_vendor_id STREQUAL "GenuineIntel")
  add_definitions(
    -DFARMHASH_ASSUME_SSE3
    -DFARMHASH_ASSUME_SSE41
    -DFARMHASH_ASSUME_SSE42
  )
endif(_vendor_id STREQUAL "GenuineIntel")

if(MSVC OR MSVC_IDE)
  add_definitions(
    -DFARMHASH_NO_BUILTIN_EXPECT
  )
endif()

include_directories(
  ${BASE}
)
