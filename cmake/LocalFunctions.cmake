################################################################################
# Generated source files.
#
#   input  - Path to the input file
#   output - Name of a CMake variable (usually list of sources)
#      where names of resulting files will be appended
################################################################################

function(PROTOC proto output)
  get_filename_component( _proto_realpath ${proto} REALPATH )
  get_filename_component( _proto_dirname  ${_proto_realpath} PATH )
  get_filename_component( _proto_basename ${_proto_realpath} NAME_WE )
  get_filename_component( _source_realpath ${CMAKE_SOURCE_DIR} REALPATH )
  string(REPLACE "${_source_realpath}" "" _relative_path "${_proto_dirname}")

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
        -I${_source_realpath}
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
endfunction()

function(PUMP pump output)
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
endfunction()

# TODO(sandello): Autodiscover binaries.
set(RAGEL_BIN /home/sandello/Cellar/bin/ragel) # Ragel v.6.8
set(BISON_BIN /home/sandello/Cellar/bin/bison) # Bison v.3.0

function(RAGEL input output)
  get_filename_component(_real_path ${input} REALPATH)
  get_filename_component(_dirname ${_real_path} PATH)
  get_filename_component(_basename ${_real_path} NAME_WE)
  set(_result ${_dirname}/${_basename}.cpp)

  if(YT_BUILD_HAVE_RAGEL)
    # Specify custom command how to generate .cpp.
    add_custom_command(
      OUTPUT
        ${_result}
      COMMAND
        ${RAGEL_BIN} -C -G2 ${_real_path} -o ${_result}
      MAIN_DEPENDENCY
        ${_real_path}
      WORKING_DIRECTORY
        ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generating Ragel automata from ${input}..."
    )
  endif()

  # Append generated .cpp to the output variable.
  set(${output} ${${output}} ${_result} PARENT_SCOPE)
endfunction()

function(BISON input output)
  get_filename_component(_real_path ${input} REALPATH)
  get_filename_component(_dirname ${_real_path} PATH)
  get_filename_component(_basename ${_real_path} NAME_WE)
  set(_result ${_dirname}/${_basename}.cpp)
  set(_result_aux ${_dirname}/${_basename}.hpp ${_dirname}/stack.hh)

  if (YT_BUILD_HAVE_BISON)
    # Specify custom command how to generate .cpp.
    add_custom_command(
      OUTPUT
        ${_result} ${_result_aux}
      COMMAND
        ${BISON_BIN} -fcaret ${_real_path} -o ${_result}
      MAIN_DEPENDENCY
        ${_real_path}
      WORKING_DIRECTORY
        ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generating Bison parser from ${input}..."
    )
  endif()

  # Append generated .cpp to the output variable.
  set(${output} ${${output}} ${_result} ${_result_aux} PARENT_SCOPE)
endfunction()

