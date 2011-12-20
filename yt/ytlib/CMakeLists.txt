set( DIRS
  actions
  election
  chunk_holder
  chunk_client
  chunk_server
  transaction_server
  transaction_client
  logging
  meta_state
  misc
  rpc
  bus
  ytree
  cypress
  orchid
  file_server
  file_client
  table_server
  table_client
  monitoring
  driver
)

foreach( _d_ ${DIRS} )
  include( ${_d_}/CMakeLists.txt )
  source_group( ${_d_} REGULAR_EXPRESSION "${_d_}/.*\\.((cpp)|(h)|(proto))")
endforeach()

add_msvc_precompiled_header( "stdafx.h" "stdafx.cpp" YT_SOURCE_FILES )
add_library( ytlib STATIC
  ${YT_SOURCE_FILES}
)

target_link_libraries ( ytlib
  arcadia-utils

  qmisc
  netliba_v6
  json

  # TODO(sandello): arcadia-utils should depend on these targets.
  snappy
  lz4
  fastlz
  minilzo
  arczlib
  protobuf
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}/chunk_server
  ${CMAKE_CURRENT_BINARY_DIR}/chunk_holder
  ${CMAKE_CURRENT_BINARY_DIR}/rpc
  ${CMAKE_CURRENT_BINARY_DIR}/ytree
  ${CMAKE_CURRENT_BINARY_DIR}/file_server
  ${CMAKE_CURRENT_BINARY_DIR}/file_client
)

if ( NOT "${CMAKE_CURRENT_BINARY_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}" )
  include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}/chunk_client
    ${CMAKE_CURRENT_BINARY_DIR}/election
    ${CMAKE_CURRENT_BINARY_DIR}/transaction_server
    ${CMAKE_CURRENT_BINARY_DIR}/meta_state
    ${CMAKE_CURRENT_BINARY_DIR}/cypress
    ${CMAKE_CURRENT_BINARY_DIR}/orchid
    ${CMAKE_CURRENT_BINARY_DIR}/table_server
    ${CMAKE_CURRENT_BINARY_DIR}/table_client
  )
endif()

#SET(USE_LF_ALLOCATOR yes)
