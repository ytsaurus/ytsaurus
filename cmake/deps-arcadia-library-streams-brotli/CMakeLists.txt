set(NAME deps-arcadia-library-streams-brotli)
set(BASE ${CMAKE_SOURCE_DIR}/library/streams/brotli)
set(SRCS
  ${BASE}/brotli.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-contrib-libs-brotli
  deps-arcadia-util
)
