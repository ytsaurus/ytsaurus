set( DIRS
  actions
  concurrency
  compression
  erasure
  logging
  misc
  rpc
  bus
  ytree
  yson
  ypath
  profiling
)

foreach( _d_ ${DIRS} )
  include( ${_d_}/CMakeLists.txt )
  source_group( ${_d_} REGULAR_EXPRESSION "${_d_}/.*\\.((cpp)|(h)|(proto))")
endforeach()

add_msvc_precompiled_header(
  stdafx.h
  stdafx.cpp
  YT_SOURCE_FILES
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/build.cpp.in
  ${CMAKE_CURRENT_BINARY_DIR}/build.cpp
)

list(APPEND YT_SOURCE_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/build.h
  ${CMAKE_CURRENT_BINARY_DIR}/build.cpp
)

add_library(ytcore STATIC
  ${YT_SOURCE_FILES}
)

target_link_libraries ( ytcore
  ytext-arcadia-util
  ytext-ev
  ytext-jerasure
  ytext-json
  ytext-httpserver
  protobuf
)

if (HAVE_LIBUNWIND)
  target_link_libraries( ytcore unwind )
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_SOURCE_DIR}/contrib/tclap
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)
