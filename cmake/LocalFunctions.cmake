
# Generate C++ .h, .cc from ptotobuf description
#   proto  - .proto file
#   srcgen - name of CMake variable (possibly with list of sources)
#            where names of .pb.h and .pb.cc will be added

function( PROTOC proto srcgen )
  get_filename_component( rp ${proto} PATH )
  if ( "${rp}" STREQUAL "" )
    SET( rp ${CMAKE_CURRENT_SOURCE_DIR} )
    SET( ap_proto ${rp}/${proto} )
  else()
    SET( ap_proto ${proto} )
  endif()
  get_filename_component( basename ${proto} NAME_WE )
  string( REPLACE "${CMAKE_SOURCE_DIR}" "" notop "${rp}" )

  # command "protoc blah-blah-blah"
  if (NOT WIN32)
    SET( _protoc_ ${CMAKE_BINARY_DIR}/bin/protoc -I${CMAKE_BINARY_DIR}${notop} --cpp_out=${CMAKE_BINARY_DIR}${notop} --cpp_styleguide_out=${CMAKE_BINARY_DIR}${notop} --plugin=protoc-gen-cpp_styleguide=${CMAKE_BINARY_DIR}/bin/cpp_styleguide ${ap_proto} )
  else()
    SET( _protoc_ ${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}/protoc -I${CMAKE_BINARY_DIR}${notop} --cpp_out=${CMAKE_BINARY_DIR}${notop} --cpp_styleguide_out=${CMAKE_BINARY_DIR}${notop} --plugin=protoc-gen-cpp_styleguide=${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}/cpp_styleguide ${ap_proto} )
  endif()

  # add generated .pb.h and .pb.cc into source list ${${srcgen}}
  SET( ${srcgen} ${${srcgen}} ${CMAKE_BINARY_DIR}${notop}/${basename}.pb.h ${CMAKE_BINARY_DIR}${notop}/${basename}.pb.cc PARENT_SCOPE )

  # custom command, how to generate .pb.h and .pb.cc
  add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}${notop}/${basename}.pb.h ${CMAKE_BINARY_DIR}${notop}/${basename}.pb.cc
    COMMAND ${_protoc_}
    MAIN_DEPENDENCY ${proto}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating protobuf"
  )
endfunction( PROTOC )
