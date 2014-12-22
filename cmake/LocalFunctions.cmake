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
  get_filename_component( filename ${_source_path} NAME )
  string(REPLACE ".pump" "" _target_filename "${filename}")
  set(_target_path ${CMAKE_BINARY_DIR}/include/${_target_filename})

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

function(RAGEL source result_variable)
  get_filename_component(_realpath ${source} REALPATH)
  get_filename_component(_dirname ${_realpath} PATH)
  get_filename_component(_basename ${_realpath} NAME_WE)
  set(_input ${_realpath})
  set(_output ${_dirname}/${_basename}.cpp)

  if(YT_BUILD_HAVE_RAGEL)
    INCLUDE(FindPerl)
    # Specify custom command how to generate .cpp.
    add_custom_command(
      OUTPUT
        ${_output}
      COMMAND
        ${RAGEL_EXECUTABLE} -C -G2 ${_input} -o ${_output}
      COMMAND
        ${PERL_EXECUTABLE} -ni -e 'print unless /^\#line/' ${_output}
      MAIN_DEPENDENCY
        ${_input}
      WORKING_DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT
        "Generating Ragel automata from ${input}..."
    )
  endif()

  set(${result_variable} ${${result_variable}} ${_output} PARENT_SCOPE)
endfunction()

function(BISON source result_variable)
  get_filename_component(_realpath ${source} REALPATH)
  get_filename_component(_dirname ${_realpath} PATH)
  get_filename_component(_basename ${_realpath} NAME_WE)

  if (YT_BUILD_HAVE_BISON)
    INCLUDE(FindPerl)
    # Specify custom command how to generate .cpp.
    add_custom_command(
      OUTPUT
        ${_dirname}/${_basename}.cpp
        ${_dirname}/${_basename}.hpp
        ${_dirname}/stack.hh
      COMMAND
        ${BISON_EXECUTABLE} --locations -fcaret ${_realpath} -o ${_dirname}/${_basename}.cpp
      COMMAND
        ${PERL_EXECUTABLE} -ni -e 'print unless /^\#line/' ${_dirname}/${_basename}.cpp
      COMMAND
        ${PERL_EXECUTABLE} -ni -e 'print unless /^\#line/' ${_dirname}/${_basename}.hpp
      COMMAND
        ${PERL_EXECUTABLE} -ni -e 'print unless /^\#line/' ${_dirname}/stack.hh
      MAIN_DEPENDENCY
        ${_realpath}
      WORKING_DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT
        "Generating Bison parser from ${source}..."
    )
  endif()

  set(
    ${result_variable}
    ${${result_variable}}
    ${_dirname}/${_basename}.cpp
    ${_dirname}/${_basename}.hpp
    ${_dirname}/stack.hh
    PARENT_SCOPE
  )
endfunction()

function(PERLXSCPP source result_variable)
  get_filename_component(_realpath ${source} REALPATH)
  get_filename_component(_dirname ${_realpath} PATH)
  get_filename_component(_basename ${_realpath} NAME_WE)
  set(_input ${_realpath})
  string(REPLACE ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR} _output ${_dirname}/${_basename}.xs.cpp)

  if (NOT TARGET ${CMAKE_CURRENT_BINARY_DIR}/ppport.h)
    add_custom_command(
      OUTPUT
        ${CMAKE_CURRENT_BINARY_DIR}/ppport.h
      COMMAND
        ${PERL_EXECUTABLE}
          -mDevel::PPPort
          -eDevel::PPPort::WriteFile
      WORKING_DIRECTORY
        ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT
        "Generating ppport.h..."
    )
  endif()

  add_custom_command(
    OUTPUT
      ${_output}
    COMMAND
      ${PERL_EXECUTABLE}
        ${PERL_PRIVLIB}/ExtUtils/xsubpp
        -typemap ${PERL_PRIVLIB}/ExtUtils/typemap
        -csuffix .cpp
        -prototypes
        -hiertype
        -output ${_output}
        ${_input}
    MAIN_DEPENDENCY
      ${_input}
    DEPENDS
      ${_dirname}/typemap
    WORKING_DIRECTORY
      ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT
      "Building ${source} with perl (.xs.cpp)..."
  )

  set_source_files_properties(
    ${_output}
    PROPERTIES
    COMPILE_FLAGS "${PERL_EXTRA_C_FLAGS} -Wno-unused-variable -Wno-literal-suffix"
    GENERATED TRUE
  )

  set(${result_variable} ${${result_variable}} ${_output} PARENT_SCOPE)
endfunction()

