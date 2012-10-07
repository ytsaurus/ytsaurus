set( DIRS
  actions
  election
  chunk_client
  codecs
  transaction_client
  logging
  meta_state
  misc
  rpc
  bus
  ytree
  ypath
  cypress_client
  orchid
  file_client
  table_client
  object_client
  monitoring
  profiling
  driver
  formats
  scheduler
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

add_library( ytlib STATIC
  ${YT_SOURCE_FILES}
)

target_link_libraries ( ytlib
  ytext-arcadia-util
  ytext-json
  ytext-uv
  protobuf
)

if (HAVE_LIBUNWIND)
  target_link_libraries( ytlib libunwind.a )
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}

  ${CMAKE_SOURCE_DIR}/yt

  ${CMAKE_SOURCE_DIR}/contrib/tclap
  ${CMAKE_SOURCE_DIR}/contrib/libuv/include
  ${CMAKE_SOURCE_DIR}/contrib/libuv/include/uv-private
  ${CMAKE_SOURCE_DIR}/contrib/libuv/src

  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)
