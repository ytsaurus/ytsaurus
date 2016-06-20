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
    elseif(${_arg} STREQUAL "INCLUDE_DIRECTORIES")
      set(_list _include_dirs)
    else()
      set(${_list} ${${_list}} ${_arg})
    endif()
  endforeach()

  set(_inter_dirname ${CMAKE_CURRENT_BINARY_DIR})
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
  set(_include_dir ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client/udf)
  set(_dirs ${_include_dirs} ${_dirs} ${_include_dir})
  set(_include_dirs)

  foreach(_dir ${_dirs})
    set(_include_dirs ${_include_dirs} -I${_dir})
  endforeach()

  if(${_extension} STREQUAL ".cpp")
    set(_compiler ${CLANGPP_EXECUTABLE})
    set(_options -std=c++1y -Wglobal-constructors)
    set(_depends
        ${_include_dir}/yt_udf_cpp.h
    )
    set(_lang "CXX")
  else()
    set(_compiler ${CLANG_EXECUTABLE})
    set(_depends
        ${_include_dir}/yt_udf.h
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
          -g
          -DYT_COMPILING_UDF
          -DNDEBUG
          ${_options}
          ${_include_dirs}
          $$f\;
      done
    COMMAND
      ${LLVM_LINK_EXECUTABLE}
        -o ${_bc_filename}.tmp
        ${_bc_filename} ${_extra_bc_filenames}
    COMMAND
      mv ${_bc_filename}.tmp ${_bc_filename}
    COMMAND
      ${LLVM_OPT_EXECUTABLE}
        -O2
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
      ${CLANGPP_EXECUTABLE}
      ${CLANG_EXECUTABLE}
      ${LLVM_OPT_EXECUTABLE}
      ${LLVM_LINK_EXECUTABLE}
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
  get_filename_component(_proto_realpath ${proto} REALPATH)
  get_filename_component(_proto_dirname  ${_proto_realpath} PATH)
  get_filename_component(_proto_basename ${_proto_realpath} NAME_WE)
  get_filename_component(_source_realpath ${CMAKE_SOURCE_DIR} REALPATH)
  string(REPLACE "${_source_realpath}" "" _relative_path "${_proto_dirname}")

  # Specify custom command how to generate .pb.h and .pb.cc.
  add_custom_command(
    OUTPUT
      ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.h
      ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.cc
    COMMAND
      ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}${_relative_path}
    COMMAND
    $<TARGET_FILE:protoc>
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

  set_source_files_properties(
    ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.h
    ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.cc
    PROPERTIES GENERATED TRUE
  )

  # Append generated .pb.h and .pb.cc to the output variable.
  set(${output}
    ${${output}}
    ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.h
    ${CMAKE_BINARY_DIR}${_relative_path}/${_proto_basename}.pb.cc
    PARENT_SCOPE)
endfunction()

function(RAGEL source result_variable)
  get_filename_component(_realpath ${source} REALPATH)
  get_filename_component(_dirname ${_realpath} PATH)
  get_filename_component(_basename ${_realpath} NAME_WE)
  set(_input ${_realpath})
  set(_output ${_dirname}/${_basename}.cpp)

  if(YT_BUILD_HAVE_RAGEL)
    include(FindPerl)
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
    set_source_files_properties(
      ${_output}
      PROPERTIES GENERATED TRUE
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
    set_source_files_properties(
      ${_output}
      PROPERTIES GENERATED TRUE
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

function(RESOLVE_SRCS srcs output)
  set(_o_)
  foreach(_s_ ${${srcs}})
    if(_s_ MATCHES "\\.cpp$")
      list(APPEND _o_ "${_s_}")
    elseif(_s_ MATCHES "\\.proto$")
      protoc("${_s_}" _o_)
    elseif(_s_ MATCHES "\\.S$")
      list(APPEND _o_ "${_s_}")
      set_source_files_properties("${_s_}" PROPERTIES LANGUAGE C)
    else()
      message(FATAL_ERROR "Cannot handle source file ${_s_}")
    endif()
  endforeach()
  set(${output} ${${output}} ${_o_} PARENT_SCOPE)
endfunction()

function(ADD_GDB_INDEX target)
  get_target_property(_location ${target} LOCATION_${CMAKE_BUILD_TYPE})
  get_filename_component(_dirname ${_location} PATH)
  get_filename_component(_name ${_location} NAME)

  find_program(GDB_EXECUTABLE
    NAMES gdb
    PATHS /usr/bin
  )

  if(GDB_EXECUTABLE-NOTFOUND)
    message(STATUS "Failed to find gdb executable, gdb index will not be built")
    return()
  endif()

  if(CMAKE_OBJCOPY-NOTFOUND)
    message(STATUS "Failed to find objcopy binary, gdb index will not be built")
    return()
  endif()

  add_custom_command(
    TARGET ${target}
    POST_BUILD
    COMMAND
      ${GDB_EXECUTABLE} ${_location}
        -batch -n
        --ex "save gdb-index ${_dirname}"
    COMMAND
      ${CMAKE_OBJCOPY}
        --add-section .gdb_index="${_location}.gdb-index"
        --set-section-flags .gdb_index=readonly
        ${_location}
    COMMAND
      ${CMAKE_COMMAND} -E remove "${_location}.gdb-index"
    COMMENT
        "Building gdb index for ${_name}..."
  )

endfunction()
