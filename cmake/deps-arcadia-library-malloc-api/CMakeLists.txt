set(NAME deps-arcadia-library-malloc-api)
set(BASE ${CMAKE_SOURCE_DIR}/library/malloc/api)
set(SRCS
  ${BASE}/malloc.cpp
)

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME} deps-arcadia-util)
