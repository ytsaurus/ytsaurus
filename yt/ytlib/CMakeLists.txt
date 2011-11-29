
include(actions/CMakeLists.txt)
include(election/CMakeLists.txt)
include(chunk_holder/CMakeLists.txt)
include(chunk_client/CMakeLists.txt)
include(chunk_server/CMakeLists.txt)
include(transaction_server/CMakeLists.txt)
include(transaction_client/CMakeLists.txt)
include(logging/CMakeLists.txt)
include(meta_state/CMakeLists.txt)
include(misc/CMakeLists.txt)
include(rpc/CMakeLists.txt)
include(bus/CMakeLists.txt)
include(ytree/CMakeLists.txt)
include(cypress/CMakeLists.txt)
include(orchid/CMakeLists.txt)
include(file_server/CMakeLists.txt)
include(file_client/CMakeLists.txt)
include(table_server/CMakeLists.txt)
include(table_client/CMakeLists.txt)
include(monitoring/CMakeLists.txt)

add_library( ytlib STATIC
  ${YT_SOURCE_FILES}
  stdafx.cpp
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  .
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/chunk_server
  ${CMAKE_CURRENT_SOURCE_DIR}/chunk_holder
  ${CMAKE_CURRENT_SOURCE_DIR}/rpc
  ${CMAKE_CURRENT_SOURCE_DIR}/ytree
  ${CMAKE_CURRENT_SOURCE_DIR}/file_server
  ${CMAKE_CURRENT_SOURCE_DIR}/file_client
)

add_dependencies( ytlib
  protoc
  cpp_styleguide
)

#
#SET(USE_LF_ALLOCATOR yes)

