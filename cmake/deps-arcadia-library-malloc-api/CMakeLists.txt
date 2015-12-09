set(BASE ${CMAKE_SOURCE_DIR}/library/malloc/api)
set(SRCS
  ${BASE}/malloc.cpp
)

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_library(ytext-arcadia-library-malloc-api STATIC ${SRCS})

target_link_libraries(ytext-arcadia-library-malloc-api
  ytext-arcadia-util
)

