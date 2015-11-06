set( BASE ${CMAKE_SOURCE_DIR}/library/streams/lz )
set( SRCS
  ${BASE}/lz.cpp
)

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_library( ytext-arcadia-library-streams-lz STATIC ${SRCS} )

target_link_libraries( ytext-arcadia-library-streams-lz
  ytext-arcadia-util
)

