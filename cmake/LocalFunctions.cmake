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

find_program(RAGEL_EXECUTABLE ragel)
mark_as_advanced(RAGEL_EXECUTABLE)

if(YT_BUILD_HAVE_RAGEL)
  if(NOT RAGEL_EXECUTABLE)
    message(FATAL_ERROR "Could not find Ragel.")
  endif()

  execute_process(
    COMMAND ${RAGEL_EXECUTABLE} --version
    OUTPUT_VARIABLE RAGEL_VERSION
  )

  if(NOT RAGEL_VERSION MATCHES "6.8")
    message(FATAL_ERROR "Ragel 6.8 is required.")
  endif()

  message(STATUS "Found Ragel: ${RAGEL_EXECUTABLE}")
endif()

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
        ${RAGEL_EXECUTABLE} -C -G2 ${_real_path} -o ${_result}
      COMMAND
        perl -ni -e 'print unless /^\#line/' ${_result}
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

find_program(BISON_EXECUTABLE bison)
mark_as_advanced(BISON_EXECUTABLE)

if(YT_BUILD_HAVE_BISON)
  if(NOT BISON_EXECUTABLE)
    message(FATAL_ERROR "Could not find Bison.")
  endif()

  execute_process(
    COMMAND ${BISON_EXECUTABLE} --version
    OUTPUT_VARIABLE BISON_VERSION
  )

  if(NOT BISON_VERSION MATCHES "3.0")
    message(FATAL_ERROR "Bison 3.0 is required.")
  endif()

  message(STATUS "Found Bison: ${BISON_EXECUTABLE}")
endif()

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
        ${BISON_EXECUTABLE} --locations -fcaret ${_real_path} -o ${_result}
      COMMAND
        perl -ni -e 'print unless /^\#line/' ${_dirname}/${_basename}.cpp
      COMMAND
        perl -ni -e 'print unless /^\#line/' ${_dirname}/${_basename}.hpp
      COMMAND
        perl -ni -e 'print unless /^\#line/' ${_dirname}/stack.hh
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

