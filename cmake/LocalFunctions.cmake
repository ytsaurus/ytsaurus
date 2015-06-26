################################################################################
# Generated source files.
#
#   input  - Path to the input file
#   output - Name of a CMake variable (usually list of sources)
#      where names of resulting files will be appended
################################################################################

# Can be called with extra arguments to specify which symbols to export and
# which files to link in. If no symbols are specified, it is assumed that the
# filename is the name of the UD(A)F being defined (e.g. is_null.c defines
# is_null).
#
# udf(udf output type
#     [[SYMBOLS] symbols...]
#     [FILES files...])
#
function(UDF udf output type)
  get_filename_component( _realpath ${udf} REALPATH )
  get_filename_component( _filename ${_realpath} NAME_WE )
  get_filename_component( _extension ${_realpath} EXT )

  set(_h_dirname ${CMAKE_BINARY_DIR}/include/udf)
  set(_h_file ${_h_dirname}/${_filename}.h)
  set(${output} ${${output}} ${_h_file} PARENT_SCOPE)

  set(_extraargs ${ARGN})
  set(_list _extrasymbols_list)
  foreach(_arg ${_extraargs})
    if(${_arg} STREQUAL "SYMBOLS")
      set(_list _extrasymbols_list)
    elseif(${_arg} STREQUAL "FILES")
      set(_list _extrafiles)
    else()
      set(${_list} ${${_list}} ${_arg})
    endif()
  endforeach()

  set(_inter_dirname ${CMAKE_BINARY_DIR}/bin/)
  set(_inter_filename ${_filename}.${type})
  set(_bc_filename ${_filename}.bc)
  foreach(_file ${_extrafiles})
    get_filename_component( _extra_realpath ${_file} REALPATH )
    get_filename_component( _extra_filename ${_extra_realpath} NAME_WE )
    set(_extra_bc_filenames ${_extra_bc_filenames} ${_extra_filename}.bc)
  endforeach()

  foreach(_symbol ${_extrasymbols_list})
    set(_extrasymbols ${_extrasymbols},${_symbol})
  endforeach()

  get_property( _dirs
    DIRECTORY
      ${CMAKE_SOURCE_DIR}/yt
    PROPERTY
      INCLUDE_DIRECTORIES
  )
  foreach( _dir ${_dirs})
    set(_include_dirs ${_include_dirs} -I${_dir})
  endforeach()
  set(_include_dir ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client/udf)
  set(_include_dirs ${_include_dirs} -I${_include_dir})

  find_program(CLANG_EXECUTABLE
    NAMES clang-3.6 clang
    PATHS $ENV{LLVM_ROOT}/bin
  )
  find_program(CLANGPP_EXECUTABLE
    NAMES clang++-3.6 clang++
    PATHS $ENV{LLVM_ROOT}/bin
  )
  find_program(LINK_EXECUTABLE
    NAMES llvm-link-3.6 llvm-link
    PATHS $ENV{LLVM_ROOT}/bin
  )
  find_program(OPT_EXECUTABLE
    NAMES opt-3.6 opt
    PATHS $ENV{LLVM_ROOT}/bin
  )

  if(${_extension} STREQUAL ".cpp") 
    set(_compiler ${CLANGPP_EXECUTABLE})
    set(_options -std=c++1y -Wglobal-constructors)
    set(_depends
        ${_include_dir}/yt_udf_cpp.h
        ${_include_dir}/yt_udf_types.h
    )
    set(_lang "CXX")
  else()
    set(_compiler ${CLANG_EXECUTABLE})
    set(_depends
        ${_include_dir}/yt_udf_cpp.h
        ${_include_dir}/yt_udf_types.h
    )
    set(_lang "C")
  endif()

  add_custom_command(
    OUTPUT
      ${_h_file}
    COMMAND
      ${CMAKE_COMMAND} -E make_directory ${_h_dirname}
    COMMAND
      for f in ${_realpath} ${_extrafiles}\; do
        ${_compiler} -c
          -emit-llvm
          -DYT_COMPILING_UDF
          ${_options}
          ${_include_dirs}
          $$f\;
      done
    COMMAND
      ${LINK_EXECUTABLE}
        -o ${_bc_filename}.tmp
        ${_bc_filename} ${_extra_bc_filenames}
    COMMAND
      mv ${_bc_filename}.tmp ${_bc_filename}
    COMMAND
      ${OPT_EXECUTABLE}
        -internalize
        -internalize-public-api-list=${_filename},${_filename}_init,${_filename}_update,${_filename}_merge,${_filename}_finalize,${_extrasymbols}
        -globalopt
        -globaldce
        -o ${_bc_filename}.tmp
        ${_bc_filename}
    COMMAND
      mv ${_bc_filename}.tmp ${_bc_filename}
    COMMAND
      test ${type} = "o"
        && ${CLANG_EXECUTABLE} -c -fPIC -o ${_inter_filename} ${_bc_filename}
        && rm ${_bc_filename}
        || true
    COMMAND
      xxd -i ${_inter_filename} > ${_h_file}
    MAIN_DEPENDENCY
      ${_realpath}
    DEPENDS
      ${_depends}
      ${_extrafiles}
    IMPLICIT_DEPENDS
      ${_lang} ${_realpath} ${_extrafiles}
    WORKING_DIRECTORY
      ${_inter_dirname}
    COMMENT "Generating UDF header for ${_filename}..."
  )
endfunction()

macro(UDF_BC udf_impl output)
    udf(${udf_impl} ${output} bc ${ARGN})
endmacro()

macro(UDF_O udf_impl output)
    udf(${udf_impl} ${output} o ${ARGN})
endmacro()

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

  get_property(protoc_location TARGET protoc PROPERTY LOCATION)

  # Specify custom command how to generate .pb.h and .pb.cc.
  add_custom_command(
    OUTPUT
      ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.h
      ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.cc
    COMMAND
      ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}${_relative_path}
    COMMAND
      ${protoc_location}
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

