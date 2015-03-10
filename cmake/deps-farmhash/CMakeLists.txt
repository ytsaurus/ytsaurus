set( BASE ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash )

set( SRCS_FARMHASH
  ${BASE}/farmhash.cc
)

add_library( farmhash STATIC
  ${SRCS_FARMHASH}
)

if(_vendor_id STREQUAL "GenuineIntel")
  add_dependencies( farmhash
    -DFARMHASH_ASSUME_SSE3
    -DFARMHASH_ASSUME_SSE41
    -DFARMHASH_ASSUME_SSE42
  )
endif(_vendor_id STREQUAL "GenuineIntel")


include_directories(
  ${BASE}
)
