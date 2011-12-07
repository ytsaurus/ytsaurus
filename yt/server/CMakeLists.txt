add_executable( server
  stdafx.cpp
  main.cpp
  cell_master_server.cpp
  chunk_holder_server.cpp
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/include
  ${CMAKE_BINARY_DIR}/yt/ytlib/chunk_server
  ${CMAKE_BINARY_DIR}/yt/ytlib/ytree
)

if ( NOT "${CMAKE_CURRENT_BINARY_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}" )
  include_directories(
    ${CMAKE_BINARY_DIR}/yt/ytlib/chunk_holder
    ${CMAKE_BINARY_DIR}/yt/ytlib/chunk_client
    ${CMAKE_BINARY_DIR}/yt/ytlib/chunk_server
    ${CMAKE_BINARY_DIR}/yt/ytlib/election
    ${CMAKE_BINARY_DIR}/yt/ytlib/transaction_server
    ${CMAKE_BINARY_DIR}/yt/ytlib/meta_state
    ${CMAKE_BINARY_DIR}/yt/ytlib/cypress
    ${CMAKE_BINARY_DIR}/yt/ytlib/orchid
  )
endif()

target_link_libraries( server
  ytlib
)

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( server stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( server PROPERTIES LINK_FLAGS "-nodefaultlibs" )
  endif()
endif()
