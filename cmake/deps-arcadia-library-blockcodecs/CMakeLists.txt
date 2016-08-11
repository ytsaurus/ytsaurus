set(NAME deps-arcadia-library-blockcodecs)
set(BASE ${CMAKE_SOURCE_DIR}/library/blockcodecs)
set(SRCS
  ${BASE}/codecs.cpp
  ${BASE}/stream.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME} deps-arcadia-util)
