set(NAME deps-arcadia-library-streams-lz)
set(BASE ${CMAKE_SOURCE_DIR}/library/streams/lz)
set(SRCS
  ${BASE}/lz.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-contrib-libs-fastlz
  deps-arcadia-contrib-libs-lz4
  deps-arcadia-contrib-libs-minilzo
  deps-arcadia-util
)
