set(BASE ${CMAKE_SOURCE_DIR}/library/lfalloc)
set(SRCS
  ${BASE}/lf_allocX64.cpp
  ${BASE}/malloc-info.cpp
)

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_definitions(-DLFALLOC_YT)

add_library(ytext-arcadia-library-lfalloc STATIC ${SRCS})

target_link_libraries(ytext-arcadia-library-lfalloc
  ytext-arcadia-util
  ytext-arcadia-library-malloc-api
)

