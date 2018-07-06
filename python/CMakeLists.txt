function(GET_PYTHON_KIND_SPECIFIC_CONSTANTS python_kind output_python_kind_dashed output_target_name_prefix)
  string(TOLOWER ${python_kind} _python_kind_lower_)
  string(REPLACE "_" "-" _python_kind_dashed_ ${_python_kind_lower_})
  set(_target_name_prefix_ "yt-python-${_python_kind_dashed_}-")

  set(${output_python_kind_dashed} ${_python_kind_dashed_} PARENT_SCOPE)
  set(${output_target_name_prefix} ${_target_name_prefix_} PARENT_SCOPE)
endfunction()

function(BUILD_AND_INSTALL_PYTHON_LIBRARY python_kind target_name component)
  string(TOLOWER ${python_kind} PYTHON_KIND_LOWER)
  string(REPLACE "_" "-" PYTHON_KIND_DASHED ${PYTHON_KIND_LOWER})

  set(PYTHON_LINK_FLAGS "-static-libstdc++")

  if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set(
      PYTHON_LINK_FLAGS
      "${PYTHON_LINK_FLAGS} -Wl,-exported_symbols_list,${CMAKE_SOURCE_DIR}/yt/python/linker.map.darwin")
  else()
    set(
      PYTHON_LINK_FLAGS
      "${PYTHON_LINK_FLAGS} -Wl,--version-script=${CMAKE_SOURCE_DIR}/yt/python/linker.map")
  endif()

  execute_process(COMMAND lsb_release --codename --short OUTPUT_VARIABLE LSB_RELEASE)

  set(IS_NATIVE_SYSTEM_PYTHON False)
  if(${python_kind} MATCHES "^2_7$")
    if(${LSB_RELEASE} MATCHES "precise")
      set(IS_NATIVE_SYSTEM_PYTHON True)
    endif()
    if(${LSB_RELEASE} MATCHES "trusty")
      set(IS_NATIVE_SYSTEM_PYTHON True)
    endif()
    if(${LSB_RELEASE} MATCHES "xenial")
      set(IS_NATIVE_SYSTEM_PYTHON True)
    endif()
  endif()

  target_compile_definitions(${target_name} PUBLIC -DPy_LIMITED_API=0x03040000)

  set_target_properties(${target_name}
    PROPERTIES
    LIBRARY_OUTPUT_NAME "${component}_lib"
    LIBRARY_OUTPUT_DIRECTORY "${LIBRARY_OUTPUT_PATH}/pyshared-${PYTHON_KIND_DASHED}"
    PREFIX ""
    SUFFIX "${CMAKE_SHARED_MODULE_SUFFIX}"
    LINK_FLAGS "${PYTHON_LINK_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS}"
  )

  install(
    TARGETS ${target_name}
    DESTINATION lib/pyshared-${PYTHON_KIND_DASHED}/yt_${component}_bindings
    COMPONENT ${target_name}
  )

  if(${IS_NATIVE_SYSTEM_PYTHON})
    exec_program(
      ${CMAKE_COMMAND} ARGS -E create_symlink
      ${LIBRARY_OUTPUT_PATH}/pyshared-${PYTHON_KIND_DASHED}/${component}_lib${CMAKE_SHARED_MODULE_SUFFIX}
      ${CMAKE_SOURCE_DIR}/python/yt_${component}_bindings/${component}_lib${CMAKE_SHARED_MODULE_SUFFIX}
    )
  endif()

endfunction()

set(PYCXX_BASE ${CMAKE_SOURCE_DIR}/non_arcadia_contrib/pycxx)
set(PYCXX_SRCS
  ${PYCXX_BASE}/cxx_exceptions.cxx
  ${PYCXX_BASE}/cxx_extensions.cxx
  ${PYCXX_BASE}/cxxextensions.c
  ${PYCXX_BASE}/cxxsupport.cxx
  ${PYCXX_BASE}/IndirectPythonInterface.cxx
)

# Build library for each kind of Python.
foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
  string(TOLOWER ${PYTHON_KIND} PYTHON_KIND_LOWER)
  string(REPLACE "_" "-" PYTHON_KIND_DASHED ${PYTHON_KIND_LOWER})
  set(TARGET_NAME_PREFIX "yt-python-${PYTHON_KIND_DASHED}-")

  add_library(${TARGET_NAME_PREFIX}pycxx STATIC ${PYCXX_SRCS})
  target_include_directories(${TARGET_NAME_PREFIX}pycxx PUBLIC ${PYCXX_BASE} ${PYTHON_${PYTHON_KIND}_INCLUDE_DIR})
  target_compile_definitions(${TARGET_NAME_PREFIX}pycxx
    PUBLIC
      -DPYCXX_PYTHON_2TO3
      -DPYCXX_6_2_COMPATIBILITY
      -DPy_LIMITED_API=0x03040000)

endforeach()

add_subdirectory(common)
add_subdirectory(driver)
add_subdirectory(yson)
add_subdirectory(yson_shared)
