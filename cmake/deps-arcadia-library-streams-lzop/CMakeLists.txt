set(NAME deps-arcadia-library-streams-lzop)
set(BASE ${CMAKE_SOURCE_DIR}/library/streams/lzop)
set(SRCS
  ${BASE}/lzop.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-contrib-libs-minilzo
  deps-arcadia-util
)
