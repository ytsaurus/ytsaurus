################################################################################
# Generate C++ .h and .cc from the protobuf description.
#
#   proto  - Path to the .proto file
#   output - Name of a CMake variable (usually list of sources)
#      where names of .pb.h and .pb.cc will be appended

function( PROTOC proto output )
  get_filename_component( _proto_realpath ${proto} REALPATH )
  get_filename_component( _proto_dirname  ${_proto_realpath} PATH )
  get_filename_component( _proto_basename ${_proto_realpath} NAME_WE )
  string(REPLACE "${CMAKE_SOURCE_DIR}" "" _relative_path "${_proto_dirname}")

  # Append generated .pb.h and .pb.cc to the output variable.
  set(${output}
    ${${output}}
    ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.h
    ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.cc
    PARENT_SCOPE)

  # Specify custom command how to generate .pb.h and .pb.cc.
  add_custom_command(
    OUTPUT
      ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.h
      ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.cc
    COMMAND
      ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}${_relative_path}
    COMMAND
      ${EXECUTABLE_OUTPUT_PATH}/${CMAKE_CFG_INTDIR}/protoc
        -I${CMAKE_SOURCE_DIR}
        --cpp_out=${CMAKE_BINARY_DIR}
        ${_proto_realpath}
    MAIN_DEPENDENCY
      ${_proto_realpath}
    DEPENDS
      protoc
    WORKING_DIRECTORY
      ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating protobuf from ${proto}..."
  )
endfunction( PROTOC )

function( PUMP pump output )
  get_filename_component( _source_path ${pump} REALPATH )
  string(REPLACE ".pump" "" _target_path "${_source_path}")

  set(${output} ${${output}} ${_target_path} PARENT_SCOPE)

  add_custom_command(
    OUTPUT
      ${_target_path}
    COMMAND
      ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/pump.py
      ${_source_path} > ${_target_path}
    MAIN_DEPENDENCY
      ${_source_path}
    DEPENDS
      ${CMAKE_SOURCE_DIR}/scripts/pump.py
    COMMENT "Pumping ${pump}..."
)
endfunction( PUMP )