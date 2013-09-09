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
  monitoring
  profiling
  formats
)

foreach( _d_ ${DIRS} )
  include( ${_d_}/CMakeLists.txt )
  source_group( ${_d_} REGULAR_EXPRESSION "${_d_}/.*\\.((cpp)|(h)|(proto))")
endforeach()

add_msvc_precompiled_header(
  "stdafx.h"
  "stdafx.cpp"
  YT_SOURCE_FILES
)

if (YT_BUILD_YTCORE_SHARED)
  add_library( ytcore SHARED
    ${YT_SOURCE_FILES}
  )
  install(
    TARGETS ytcore
    LIBRARY DESTINATION lib COMPONENT libraries
  )
else()
  add_library( ytcore STATIC
    ${YT_SOURCE_FILES}
  )
endif()

target_link_libraries ( ytcore
  ytext-arcadia-util
  ytext-ev
  ytext-jerasure
  ytext-json
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
