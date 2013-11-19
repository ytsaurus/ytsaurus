set( BASE ${CMAKE_SOURCE_DIR}/library/blockcodecs )
set( SRCS
  ${BASE}/codecs.cpp
  ${BASE}/stream.cpp
)

include_directories( ${CMAKE_SOURCE_DIR} )

add_library( ytext-arcadia-library-blockcodecs STATIC ${SRCS} )

target_link_libraries( ytext-arcadia-library-blockcodecs ytext-arcadia-util )

